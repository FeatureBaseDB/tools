cfn-sandbox
===========

This is a Cloudformation template generator for managing the sandbox environment.

Usage
-----

### Generate Cloudformation template

```
$ make cfn-sandbox.json
```

### Create a stack

```
$ make create-stack STACK=foo
```

### Update a stack

```
$ make update-stack STACK=foo
```

### Delete a stack

```
$ make delete-stack STACK=foo
```

### Describe a stack

```
$ make describe-stack STACK=foo
```
