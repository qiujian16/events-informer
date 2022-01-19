# Informer using cloud event as the tranport layer

## start sender

sender is to read secrets from an apiserver and send cloud events

```
./bin/sender --kubeconfig /home/centos/.kube/config --kafka-endpoint 127.0.0.1:9092
```

## start syncer

syncer is to get cloud events and output the kubernetes event from informer

```
./bin/syncer --kafka-endpoint 127.0.0.1:9092
```