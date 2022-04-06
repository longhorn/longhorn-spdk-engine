# Longhorn CLI for SPDK

## Installing SPDK fork

1. Get source:

```
git clone https://github.com/keithalucas/spdk
cd spdk
git checkout longhorn
git submodule update --init
```

2. Install dependencies.  There are some dependencies needed for building
SPDK.  For ubuntu or debian, you can install them with:

```
apt-get install -y make linux-libc-dev pkg-config \
        devscripts libaio-dev libc6-dev gcc meson \
	python3-pyelftools uuid-dev libssl-dev \
	libibverbs-dev libfuse-dev libiscsi-dev \
	zlib1g-dev libfdt-dev libpcap0.8-dev \
	libncurses-dev libcunit1-dev \
	build-essential nasm autoconf libtool automake
```

3. Configure and build the SPDK fork:

```
./configure
make
```

## Running SPDK

First, we need to configure the system for SPDK.  SPDK uses Linux's huge
page feature, and it includes a script to configure the system in its 
source tree.

```
cd spdk
./scripts/setup.sh
```

Next, we need to run `spdk_tgt`.  It runs in the foreground which should be
fine when it is actually deployed in a Kubernetes cluster but we need to 
make sure it doesn't timeout if we running it in a ssh session, so I 
recommend running it in screen or tmux.

```
./build/bin/spdk_tgt
```

You can run multiple instances of SPDK on a single host by specifying 
different socket paths on the command line with `-r`.  The default socket 
path is `/var/tmp/spdk.sock`.  For example, this starts SPDK with a 
different socket:

```
./build/bin/spdk_tgt -r /var/tmp/spdk2.sock
``` 

## Getting longhorn-spdk CLI

```
git clone https://github.com/keithalucas/longhorn-spdk
cd longhorn-spdk
go build
```

## Setting up storage




```
./longhorn-spdk storage create /path/to/disk
```

This will register the specified disk with SPDK with an LVS name of "longhorn"
Each storage device registered with SPDK has an LVS or logical volume store 
name associated with it.  The default name is "longhorn" but mainly for 
testing an alternative name for the LVS can be specified:

```
./longhorn-spdk storage create -l alternative-lvs /path/to/disk
```

The `storage create` subcommand can be used to re-register a disk that was 
previously registered with SPDK.  

## Starting replicas

The `replica create` subcommand is used to create or reestablish a replica in 
SPDK.

```
./longhorn-spdk replica create name 10g
```

This will create a new replica with the name "name".  The replica
is a "local" replica because doesn't make itself available over NVMe over
Fabrics.  NVMe over Fabrics needs a listen address.  In order to change the
above example to a remote replica, the listen address needs to be specified:

```
./longhorn-spdk replica create --address 10.0.0.1 name 10g
```

Additionally, a specific NVMe-oF port can be specified with `--port xxxx`.
The default NVMe-oF port will be 4420 if not specified.

## Starting the volume

The ideal longhorn scenario will be a three replica volume with one local 
replica and two remote replicas.  All three replicas will use the default
LVS name of "longhorn".  Each remote replica will use the default NVMe-oF 
port of 4420.  To start a volume with such replicas, the command will be:

```
./longhorn-spdk volume create name --replica : --replica :10.0.0.1 --replica :10.0.0.2
```

In order to achieve flexibility in the specification of replicas, there are 
four possible fields that can be specified for a replica when creating a
replica, each delimited by `:`.  The first field is the LVS which can be empty
if it is the default value of replica.  The second field is the listen IP
address; this is empty for a local replica.  The third field is the NVMe-oF 
port which is 4420 if not specified.  The fourth field is the TCP 
communications port which is 4421 if not specified.

## Registering and mounting the volume

The nvme command can discover and connect to an NVMe-oF target.

```
nvme discover -t tcp -a 127.0.0.1 -s 4420
nvme connect -t tcp -a 127.0.0.1 -s 4420 -n nqn.2021-12.io.longhorn.volume:name
nvme list
mkfs.ext4 /dev/nvme0n1
mount /dev/nvme0n1 /mnt
```
## Three instances, three node example

1. Register storage on each node:

```
./longhorn-spdk storage create /dev/sda
```

2. Create replicas:

On the two remote nodes:

```
./longhorn-spdk replica create --address <remote-ip> volume 100g
```

On the local node:

```
./longhorn-spdk replica create volume 100g
```

3. Create the volume on the local node:

```
./longhorn-spdk volume create volume --replica : --replica :<remote-ip1> --replica :<remote-ip2>
```

5. Discover and use volume.

```
modprobe nvme-tcp
nvme discover -t tcp -a 127.0.0.1 -s 4420
nvme connect -t tcp -a 127.0.0.1 -s 4420 -n nqn.2021-12.io.longhorn.volume:volume
mkfs.ext4 /dev/nvme0n1
mount /dev/nvme0n1 /mnt

```
## One instance, file based disks example

This example uses virtual disks to 

1. Create file based disks:

```
fallocate -l 10g file1.img
fallocate -l 10g file2.img
fallocate -l 10g file3.img
```

2. Create storage with a unique LVS name.

```
./longhorn-spdk storage create -l longhorn1 $PWD/file1.img
./longhorn-spdk storage create -l longhorn2 $PWD/file2.img
./longhorn-spdk storage create -l longhorn3 $PWD/file3.img
```

3. Create a three replicas; one in each LVS.
```
./longhorn-spdk replica create -l longhorn1 volume 5g
./longhorn-spdk replica create -l longhorn2 volume 5g
./longhorn-spdk replica create -l longhorn3 volume 5g
```

4. Create volume.
```
./longhorn-spdk volume create volume --replica longhorn1 --replica longhorn2 --replica longhorn3
```

5. Discover and use volume.

```
modprobe nvme-tcp
nvme discover -t tcp -a 127.0.0.1 -s 4420
nvme connect -t tcp -a 127.0.0.1 -s 4420 -n nqn.2021-12.io.longhorn.volume:volume
mkfs.ext4 /dev/nvme0n1
mount /dev/nvme0n1 /mnt
```
