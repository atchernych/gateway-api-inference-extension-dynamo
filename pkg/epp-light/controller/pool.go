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

package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"
	epplight "sigs.k8s.io/gateway-api-inference-extension/pkg/epp-light"
)

// InferencePoolReconciler watches InferencePool resources and updates the datastore.
type InferencePoolReconciler struct {
	client.Reader
	Datastore     epplight.Datastore
	PoolNamespace string
	PoolName      string
}

func (c *InferencePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling InferencePool")

	pool := &v1.InferencePool{}
	if err := c.Get(ctx, req.NamespacedName, pool); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("InferencePool not found, clearing datastore")
			c.Datastore.Clear()
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("unable to get InferencePool: %w", err)
	}

	if !pool.GetDeletionTimestamp().IsZero() {
		logger.Info("InferencePool is marked for deletion, clearing datastore")
		c.Datastore.Clear()
		return ctrl.Result{}, nil
	}

	poolInfo := inferencePoolToPoolInfo(pool)
	if err := c.Datastore.PoolSet(ctx, c.Reader, poolInfo); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update datastore: %w", err)
	}

	return ctrl.Result{}, nil
}

func (c *InferencePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1.InferencePool{}).
		Complete(c)
}

// PoolNamespacedName returns the namespaced name for the pool this reconciler watches.
func (c *InferencePoolReconciler) PoolNamespacedName() types.NamespacedName {
	return types.NamespacedName{Name: c.PoolName, Namespace: c.PoolNamespace}
}

func inferencePoolToPoolInfo(pool *v1.InferencePool) *epplight.PoolInfo {
	targetPorts := make([]int, 0, len(pool.Spec.TargetPorts))
	for _, p := range pool.Spec.TargetPorts {
		targetPorts = append(targetPorts, int(p.Number))
	}
	selector := make(map[string]string, len(pool.Spec.Selector.MatchLabels))
	for k, v := range pool.Spec.Selector.MatchLabels {
		selector[string(k)] = string(v)
	}
	return &epplight.PoolInfo{
		Name:        pool.Name,
		Namespace:   pool.Namespace,
		Selector:    selector,
		TargetPorts: targetPorts,
	}
}
