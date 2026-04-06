/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package epplight

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var errPoolNotSynced = errors.New("InferencePool is not initialized in data store")

const (
	// activePortsAnnotation specifies which ports on a pod are active for inference traffic.
	// Value is a comma-separated list of port numbers, e.g. "8000,8001".
	activePortsAnnotation = "inference.networking.k8s.io/active-ports"
)

// PoolInfo holds the essential information from an InferencePool resource.
type PoolInfo struct {
	Name        string
	Namespace   string
	Selector    map[string]string
	TargetPorts []int
}

// Datastore is the internal interface for the light EPP's pod/pool cache.
type Datastore interface {
	PoolGet() (*PoolInfo, error)
	PoolSet(ctx context.Context, reader client.Reader, pool *PoolInfo) error
	PoolHasSynced() bool
	PoolLabelsMatch(podLabels map[string]string) bool
	ListEndpoints() []Endpoint
	PodUpdateOrAddIfNotExist(ctx context.Context, pod *corev1.Pod) bool
	PodDelete(podName string)
	Clear()
}

// NewDatastore creates a new datastore instance.
func NewDatastore() *datastore {
	return &datastore{
		pods: &sync.Map{},
	}
}

type datastore struct {
	mu   sync.RWMutex
	pool *PoolInfo
	// key: types.NamespacedName (endpoint name), value: Endpoint
	pods *sync.Map
}

func (ds *datastore) Clear() {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.pool = nil
	ds.pods.Clear()
}

// PoolSet sets the pool in the datastore. If the selector or targetPorts changed,
// a full pod resync is triggered.
func (ds *datastore) PoolSet(ctx context.Context, reader client.Reader, pool *PoolInfo) error {
	if pool == nil {
		ds.Clear()
		return nil
	}
	logger := log.FromContext(ctx)
	ds.mu.Lock()
	defer ds.mu.Unlock()

	oldPool := ds.pool
	ds.pool = pool

	selectorChanged := oldPool == nil || !labels.Equals(oldPool.Selector, pool.Selector)
	targetPortsChanged := oldPool != nil && !slicesEqual(oldPool.TargetPorts, pool.TargetPorts)

	if selectorChanged || targetPortsChanged {
		logger.Info("Updating endpoints", "selector", pool.Selector, "targetPortsChanged", targetPortsChanged)
		if err := ds.podResyncAll(ctx, reader); err != nil {
			return fmt.Errorf("failed to resync pods: %w", err)
		}
	}
	return nil
}

func (ds *datastore) PoolGet() (*PoolInfo, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	if ds.pool == nil {
		return nil, errPoolNotSynced
	}
	return ds.pool, nil
}

func (ds *datastore) PoolHasSynced() bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.pool != nil
}

func (ds *datastore) PoolLabelsMatch(podLabels map[string]string) bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	if ds.pool == nil {
		return false
	}
	return labels.SelectorFromSet(ds.pool.Selector).Matches(labels.Set(podLabels))
}

// ListEndpoints returns all endpoints currently tracked in the datastore.
func (ds *datastore) ListEndpoints() []Endpoint {
	var result []Endpoint
	ds.pods.Range(func(_, v any) bool {
		result = append(result, v.(Endpoint))
		return true
	})
	return result
}

// PodUpdateOrAddIfNotExist adds or updates endpoints for the given pod.
// Returns true if the pod already existed (all endpoints were known).
func (ds *datastore) PodUpdateOrAddIfNotExist(ctx context.Context, pod *corev1.Pod) bool {
	ds.mu.RLock()
	pool := ds.pool
	ds.mu.RUnlock()
	return ds.podUpdateOrAddIfNotExist(ctx, pod, pool)
}

func (ds *datastore) podUpdateOrAddIfNotExist(_ context.Context, pod *corev1.Pod, pool *PoolInfo) bool {
	if pool == nil {
		return true
	}

	podLabels := make(map[string]string, len(pod.GetLabels()))
	maps.Copy(podLabels, pod.GetLabels())

	activePorts := extractActivePorts(pod, pool.TargetPorts)
	allExisted := true

	for idx, port := range pool.TargetPorts {
		epName := createEndpointName(pod, idx)
		if !activePorts.Has(port) {
			// Remove endpoint if port is no longer active.
			ds.pods.Delete(epName)
			continue
		}
		ep := Endpoint{
			Address: pod.Status.PodIP,
			Port:    strconv.Itoa(port),
			Name:    pod.Namespace + "/" + pod.Name,
			Labels:  podLabels,
		}
		if _, loaded := ds.pods.LoadOrStore(epName, ep); !loaded {
			allExisted = false
		} else {
			// Update existing endpoint (labels or IP may have changed).
			ds.pods.Store(epName, ep)
		}
	}
	return allExisted
}

func (ds *datastore) PodDelete(podName string) {
	ds.pods.Range(func(k, v any) bool {
		ep := v.(Endpoint)
		// ep.Name is "namespace/name", extract just the name part.
		if parts := strings.SplitN(ep.Name, "/", 2); len(parts) == 2 && parts[1] == podName {
			ds.pods.Delete(k)
		}
		return true
	})
}

func (ds *datastore) podResyncAll(ctx context.Context, reader client.Reader) error {
	logger := log.FromContext(ctx)
	podList := &corev1.PodList{}
	if err := reader.List(ctx, podList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(ds.pool.Selector),
		Namespace:     ds.pool.Namespace,
	}); err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	activeEndpoints := sets.New[types.NamespacedName]()
	for i := range podList.Items {
		pod := &podList.Items[i]
		if !isPodReady(pod) {
			continue
		}
		for idx := range ds.pool.TargetPorts {
			activeEndpoints.Insert(createEndpointName(pod, idx))
		}
		if !ds.podUpdateOrAddIfNotExist(ctx, pod, ds.pool) {
			logger.Info("Pod added during resync", "pod", pod.Name)
		}
	}

	// Remove endpoints that are no longer active.
	ds.pods.Range(func(k, _ any) bool {
		name := k.(types.NamespacedName)
		if !activeEndpoints.Has(name) {
			logger.Info("Removing stale endpoint", "endpoint", name)
			ds.pods.Delete(k)
		}
		return true
	})
	return nil
}

// isPodReady checks if a pod is ready and not being deleted.
func isPodReady(pod *corev1.Pod) bool {
	if !pod.DeletionTimestamp.IsZero() {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// extractActivePorts returns the set of active ports for a pod.
// If the active-ports annotation is not set, all target ports are considered active.
func extractActivePorts(pod *corev1.Pod, targetPorts []int) sets.Set[int] {
	allPorts := sets.New(targetPorts...)
	portsAnnotation, ok := pod.GetAnnotations()[activePortsAnnotation]
	if !ok {
		return allPorts
	}

	activePorts := sets.New[int]()
	for portStr := range strings.SplitSeq(portsAnnotation, ",") {
		var portNum int
		_, err := fmt.Sscanf(strings.TrimSpace(portStr), "%d", &portNum)
		if err == nil && portNum > 0 && allPorts.Has(portNum) {
			activePorts.Insert(portNum)
		}
	}
	return activePorts
}

// createEndpointName creates a namespaced name for an endpoint based on pod and rank index.
func createEndpointName(pod *corev1.Pod, idx int) types.NamespacedName {
	return types.NamespacedName{
		Name:      pod.Name + "-rank-" + strconv.Itoa(idx),
		Namespace: pod.Namespace,
	}
}

// slicesEqual checks if two int slices are equal.
func slicesEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
