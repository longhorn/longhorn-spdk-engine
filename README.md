# Longhorn CLI for SPDK

## Installing SPDK fork

1. Get source:

```
git clone https://github.com/keithalucas/spdk
git checkout longhorn
cd spdk
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
nvme discover -t tcp -a 127.0.0.1 -s 4420
nvme 
