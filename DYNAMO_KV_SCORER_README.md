# Dynamo KV Scorer Plugin - Configuration Guide

## 🚨 BREAKING CHANGE: Mandatory Environment Variables

As of this version, `DYNAMO_KV_BLOCK_SIZE` is **MANDATORY** to prevent silent KV routing failures.

## 📋 Required Configuration

### **DYNAMO_KV_BLOCK_SIZE** (MANDATORY)

**This environment variable is now REQUIRED and must exactly match your model card's `kv_cache_block_size`.**

```bash
# Example: If your model card specifies kv_cache_block_size: 512
export DYNAMO_KV_BLOCK_SIZE=512
```

#### Validation Rules

- ✅ **Must be set** (no default value)
- ✅ **Must be integer** between 16 and 8192
- ✅ **Must be power of 2** (16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192)
- ✅ **Must exactly match** model card's `kv_cache_block_size`

#### Common Values

- **256** - Small models, memory-constrained environments
- **512** - Most common, good balance of performance and memory
- **1024** - Large models, high-performance deployments
- **2048** - Very large models with high memory availability

## 🔍 How to Find Your Model's Block Size

### Method 1: Check Model Card

```bash
# Look in your model deployment configuration
cat model-deployment.yaml | grep kv_cache_block_size
# Output: kv_cache_block_size: 512
```

### Method 2: Check Running Workers

```bash
# Query Dynamo worker configuration
kubectl get configmap dynamo-worker-config -o yaml | grep kv_cache_block_size
```

### Method 3: Check Model Documentation

Most model cards specify the recommended block size in their configuration files or documentation.

## 🚀 Example Deployment

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: epp-with-dynamo-kv-scorer
spec:
  template:
    spec:
      containers:
      - name: epp
        env:
        - name: DYNAMO_KV_BLOCK_SIZE
          value: "512"  # MUST match your model card!
        - name: DYNAMO_NAMESPACE
          value: "production"
        - name: DYNAMO_COMPONENT  
          value: "backend"
        - name: DYNAMO_MODEL
          value: "Qwen/Qwen3-0.6B"
```

### Docker Compose

```yaml
services:
  epp:
    environment:
      - DYNAMO_KV_BLOCK_SIZE=512  # MUST match your model card!
      - DYNAMO_NAMESPACE=production
      - DYNAMO_COMPONENT=backend
      - DYNAMO_MODEL=Qwen/Qwen3-0.6B
```

### Shell Script

```bash
#!/bin/bash
# deployment.sh

# Get block size from your model configuration
MODEL_KV_BLOCK_SIZE=$(kubectl get configmap model-config -o jsonpath='{.data.kv_cache_block_size}')

# Deploy EPP with matching block size
export DYNAMO_KV_BLOCK_SIZE=${MODEL_KV_BLOCK_SIZE}
export DYNAMO_NAMESPACE=production
export DYNAMO_COMPONENT=backend  
export DYNAMO_MODEL=Qwen/Qwen3-0.6B

./start-epp
```

## ⚠️ Error Messages

### Missing Environment Variable

```
DYNAMO_KV_BLOCK_SIZE environment variable is required but not set. 
This must match your model card's kv_cache_block_size exactly. 
Common values: 256, 512, 1024
```

### Invalid Value

```
DYNAMO_KV_BLOCK_SIZE='abc' is not a valid integer
```

### Out of Range

```
DYNAMO_KV_BLOCK_SIZE=99999 is outside reasonable range [16, 8192]
```

### Not Power of 2

```
DYNAMO_KV_BLOCK_SIZE=100 should be a power of 2 (16, 32, 64, 128, 256, 512, 1024, etc.)
```

## 🔧 Migration Guide

### Before (with silent failures)

```bash
# This would silently fail if model card had different block size
export DYNAMO_KV_BLOCK_SIZE=512  # might not match model!
```

### After (explicit configuration)

```bash
# Step 1: Check your model's actual block size
MODEL_BLOCK_SIZE=$(get-model-block-size)  # Your method here

# Step 2: Set environment variable to match exactly
export DYNAMO_KV_BLOCK_SIZE=${MODEL_BLOCK_SIZE}

# Step 3: EPP will fail fast if there's a mismatch
```

## 📊 Impact

- ✅ **Prevents silent failures** - KV routing either works correctly or fails fast
- ✅ **Explicit configuration** - Forces users to know their model's requirements  
- ✅ **Better debugging** - Clear error messages for misconfigurations
- ✅ **Performance assurance** - Guarantees KV-aware routing works when configured

## 🆘 Troubleshooting

### Q: How do I know if my block size is correct?

A: The plugin will log on startup: `Dynamo KV Scorer: Loaded mandatory DYNAMO_KV_BLOCK_SIZE=512`

### Q: What happens if I set the wrong block size?

A: KV-aware routing will fail completely, falling back to round-robin scheduling. This will degrade performance but not break functionality.

### Q: Can I still use defaults?

A: No. This is now mandatory to prevent silent misconfigurations that are hard to debug.

### Q: Why is this mandatory now?

A: Previously, wrong block sizes caused silent KV routing failures that were very difficult to diagnose. Making it mandatory forces explicit, correct configuration.
