# tcomposs

<provide short description of service here>

## TL;DR;

```bash
$ helm install <chart_folder_location>
```

### Introduction
<provide more detailed information about the service here>

### Prerequisites
- Kubernetes v1.8+
- <other dependencies or prerequisites>

### Installing the chart
To install the chart with the release name tcomposs you need to 

```$ helm install --name tcomposs <proj_root_folder>/deploy/kubernetes/tcomposs```

The command deploys the tcomposs Service on the Kubernetes cluster in the default configuration. The [configuration](#configuration) section lists the parameters that can be configured during installation.

### Uninstalling the chart

To uninstall/delete the tcomposs deployment:

```$ helm delete --purge tcomposs```

This command removes all the Kubernetes components associated with the chart and deletes the release.

### Configuration

The chart uses two global values which need to be set before installing this chart: ```global.env``` and ```global.infraReleaseName```.

The following tables lists the configurable parameters of the Platform Configuration Service chart and their default values. 
These values can be configured independently for different releases (i.e. prod, qa, dev, etc...)

Parameter                      | Description	                                    | Default
-------------------------------|--------------------------------------------------|--------------------------------
`global.env`                   | Deployment environment                           | dev
`global.registryKey`           | k8s secret for the docker registry               | talendregistry
`replicaCount`                 | Number of containers running in parallel         | 1
`image.registry`               | Docker registry                                  | registry.datapwn.com
`image.path`                   | Docker image full path                           | talend/tcomp-components-api-service-rest-all-components-master:<tag>
`image.pullPolicy`             | Image pull policy	                              | IfNotPresent
`service.type`                 | k8s service type                                 | ClusterIP
`service.defaultPort`          | k8s service port                                 | 
`javaOpts`                     | JRE options                                      | -Xmx256M
`persistence.enabled`          | Use a PVC to persist data                        | `true`
`persistence.storageClass`     | Storage class of backing PVC                     | `nil` (uses alpha storage class annotation)
`persistence.volumeClaimName`  | PVC name                                         | infra-pvc
`persistence.accessMode`       | Use volume as ReadOnly or ReadWrite              | `ReadWriteOnce`
`persistence.size`             | Size of data volume                              | `8Gi`

You can override these values at runtime using the `--set key=value[,key=value]` argument to `helm install`. For example,

```bash
$ helm install --name tcomposs \
  --set <parameter1>=<value1>,<parameter2>=<value2> \
    <proj_root_folder>/deploy/kubernetes/tcomposs
```

The above command deploys the tcomposs Service in the k8s cluster and sets the values of ...

Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart. For example,

```bash
$ helm install --name <release_name> -f <path>/values.yaml <proj_root_folder>/deploy/kubernetes/tcomposs
```

> **Tip**: You can use the default [values.yaml](values.yaml)


### Persistence - Volumes

<explain persistance requirements/definition here>


