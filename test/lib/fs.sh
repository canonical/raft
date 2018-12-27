#!/bin/sh -e

# Setup loopback disk devices to test the raft I/O implementation against
# various file systems.

types="tmpfs
ext4
xfs
btrfs
zfs
"

if [ "$1" = "setup" ]; then
    mkdir ./tmp

    i=0
    for type in $types; do
	echo -n "Creating $type loop device mount..."

	# Create the fs mount point
	mkdir "./tmp/${type}"

	if [ "$type" = "tmpfs" ]; then
	    # For tmpfs we don't need a loopback disk device.
	    sudo mount -t tmpfs -o size=32m tmpfs ./tmp/tmpfs
	else
	    # Create a loopback disk device
	    dd if=/dev/zero of="./tmp/.${type}" bs=4096 count=28672 > /dev/null 2>&1
	    sudo losetup "/dev/loop${i}" "./tmp/.${type}"

	    # Initialize the file system
	    if [ "$type" = "zfs" ]; then
		sudo zpool create raft "/dev/loop${i}"
		sudo zfs create -o mountpoint=$(pwd)/tmp/zfs raft/zfs
	    else
		sudo mkfs.${type} "/dev/loop${i}" > /dev/null 2>&1
		sudo mount "/dev/loop${i}" "./tmp/${type}"
	    fi
	fi

	sudo chown $USER "./tmp/${type}"

	echo " done"

	i=$(expr $i + 1)
    done

    exit 0
fi

if [ "$1" = "teardown" ]; then

    i=0
    for type in $types; do
	echo -n "Deleting $type loop device mount..."

	sudo umount "./tmp/${type}"
	rm -rf "./tmp/${type}"

	if [ "$type" != "tmpfs" ]; then
	    # For zfs we need to destroy the pool
	    if [ "$type" = "zfs" ]; then
		sudo zpool destroy raft
	    fi

	    # For regular file systems, remove the loopback disk device.
	    sudo losetup -d "/dev/loop${i}"
	    rm "./tmp/.${type}"
	fi

	i=$(expr $i + 1)

	echo " done"
    done

    rmdir ./tmp
    
    exit 0
fi

echo "usage: $0 setup|teardown"

exit 1
