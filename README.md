# ADA Azure Batch

Adapted from https://github.com/Azure-Samples/batch-python-quickstart

## Big todos
- refactor
- add remaining tasks
- use azure storage
- custom VM (with nvidia drivers + cuda)

## Usage
Deploy:
```
Usage: deploy-batch [OPTIONS] POOL_ID

Options:
  -j, --job-id TEXT
  --pool-node-count TEXT
  --pool-vm-size TEXT
  --std-out-fname TEXT
  --help                  Show this message and exit.
```

With the following environment variables:
`_BATCH_ACCOUNT_NAME`,
`_BATCH_ACCOUNT_KEY`,
`_BATCH_ACCOUNT_URL`,
`_STORAGE_ACCOUNT_NAME`,
`_STORAGE_ACCOUNT_KEY`,
`_CR_PASSWORD`.
