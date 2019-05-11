#!/bin/sh -e

# Setup loopback disk devices to test the raft I/O implementation against
# various file systems.

usage() {
    echo "usage: $0 setup|teardown [types]"
}

types="tmpfs
ext4
xfs
btrfs
zfs
"

if [ "${#}" -lt 1 ]; then
    usage
    exit 1
fi

cmd="${1}"

if [ "${cmd}" = "setup" ]; then
    mkdir ./tmp

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
	    loop=$(sudo losetup -f)
	    sudo losetup "${loop}" "./tmp/.${type}"

	    # Initialize the file system
	    if [ "$type" = "zfs" ]; then
		sudo zpool create raft "${loop}"
		sudo zfs create -o mountpoint=$(pwd)/tmp/zfs raft/zfs
	    else
		sudo mkfs.${type} "${loop}" > /dev/null 2>&1
		sudo mount "${loop}" "./tmp/${type}"
	    fi
	fi

	sudo chown $USER "./tmp/${type}"

	echo " done"
    done

    exit 0
fi

if [ "${cmd}" = "teardown" ]; then

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
	    loop=$(sudo losetup -a | grep ".${type}" | cut -f 1 -d :)
	    sudo losetup -d "${loop}"
	    rm "./tmp/.${type}"
	fi

	echo " done"
    done

    rmdir ./tmp
    
    exit 0
fi

usage

exit 1
