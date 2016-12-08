cfn-production
==============

This is a Cloudformation template generator for creating Pilosa clusters.

Usage
-----

### Generate Cloudformation template

```
$ make cfn-production.json
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
