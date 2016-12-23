package PVE::Storage::Custom::DRBD8Plugin;

use strict;
use warnings;
use Data::Dumper;
use IO::File;
use File::Basename;
use PVE::Tools qw(run_command trim);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);
use PVE::QemuServer;
#use PVE::Cluster qw(cfs_read_file cfs_write_file cfs_lock_file);

use base qw(PVE::Storage::Plugin);

# to be more readable
use constant {
    UNCONFIGURED => 0,
    CONNECTED => 1,
    WFCONNECTION => 2,  # wait for connection
    STANDALONE => 3,    # "up" missing
    UNKNOWN => 0,
    PRIMARY => 1,
    SECONDARY => 2,
    UPTODATE => 1,
    };

# Configuration

sub api {
    return 1;
}

sub type {
    return 'drbd8';
}

sub plugindata {
    return {
	content => [ {images => 1, rootdir => 1}, { images => 1 }],
	format => [ { raw => 1 } , 'raw' ],
    };
}

sub properties {
    return {
	    resource => {
            # The name of the DRBD8 resource to be used for this storage
            # This needs to be the name of the DRBD8 resource file defined
            # in /etc/pve/drbd.d without the .res extension
            # The resource name have to match the pattern vm-<vmid>-disk-*
            # The '*' part must not contain a whitspace character
	        description => "DRBD8 resource",
	        type => 'string',
	    },
    };
}

sub options {
    return {
	resource => { fixed => 1 },
	nodes => { optional => 1 },
	shared => { optional => 1 },
	disable => { optional => 1 },
	content => { optional => 1 },
	format => { optional => 1 },
    };
}

# Helpers

sub drbd_get_ll_dev {
    my ($scfg) = @_;

    my $device = $scfg->{resource};

    my $cmd = ['/sbin/drbdadm', 'sh-ll-dev', "$device"];
 
    my $ll_device;
    eval {
	    run_command($cmd, outfunc => sub {
	        my $line = shift;
	        $ll_device = trim($line);
            }, errmsg => "get low level device error");
    };

    my $err = $@;
    die $err if $err;

    return $ll_device;
}

sub drbd_overview {
    my ($resource) = @_;

    my $cmd = ['/usr/sbin/drbd-overview'];
 
    my $resources = {};
    eval {
        run_command($cmd, outfunc => sub {
	        my $line = shift;
	        my $res = trim($line);

            # <drbdminor>:<resource_name>/0 <conn-state> <role> <disk-state> ....
            # conn-state: Connected or WFConnection or Unconfigured
            # role: <this>/<peer> e.g. Primary/Secondary or '.' if not up
            # disk-state: UpToDate/UpToDate or '.' if not up
            if ($res =~ m/^\s*(\d+):(\S+)\/(\d+)\s*(\S+)\s*(\S+)\s*(\S+)/) {
                my $name = $2;
                my $d = {
                    minor   => $1,
                    name    => $name,
                    vol     => $3,
	                connect => UNCONFIGURED,
	                role    => UNKNOWN,
	                prole   => UNKNOWN,
	                dstate  => UNCONFIGURED,
	                pdstate => UNCONFIGURED,
                };
                if ($4 ne "Unconfigured") {
                    if ($4 eq "Connected") {
	                    $d->{connect} = CONNECTED;
                    } else {
                        if ($4 eq "StandAlone") {
	                        $d->{connect} = STANDALONE;
                        } else {
	                        $d->{connect} = WFCONNECTION;
                        }
                    }

                    my ($me, $peer) = split('/', $5);
                    $d->{role} = PRIMARY if ($me eq "Primary");
                    $d->{role} = SECONDARY if ($me eq "Secondary");
                    $d->{prole} = PRIMARY if ($peer eq "Primary");
                    $d->{prole} = SECONDARY if ($peer eq "Secondary");

                    ($me, $peer) = split('/', $6);
                    $d->{dstate} = UPTODATE if ($me eq "UpToDate");
                    $d->{pdstate} = UPTODATE if ($peer eq "UpToDate");
                }
                if (defined($resource)) {
                    $resources->{$name} = $d if ($resource eq $name);
                } else {
                    $resources->{$name} = $d;
                }
            }
        }, errmsg => "drbd-overview error");
    };

    my $err = $@;
    die $err if $err;

    return $resources;
}

# read currently only the usable size
sub drbd_status {
    my ($resource) = @_;

    my $cmd = ['/sbin/drbdsetup','status', '--verbose', '--statistics'];

    my $status = {};
    my $match = 0;
    my $name = undef;
    eval {
        run_command($cmd, outfunc => sub {
	        my $line = shift;
	        my $stat = trim($line);

            # <resource_name> role:Secondary suspended:no
            #     write-ordering:...
            #   volume:0 minor:5 ...
            #       size:<usable_size_in_kbytes> ...
            #   .....
            if ($stat =~ m/^(\S+)\s*role:/) {
                $name = $1;
                $match = 1;
            }
            if (($match == 1) && ($stat =~ m/^\s*size:(\d+)/)) {
                my $d = {
                    name  => $name,
                    # convert to bytes
	                size  => $1*1024,
                };
                if (defined($resource)) {
                    $status->{$name} = $d if ($resource eq $name);
                } else {
                    $status->{$name} = $d;
                }
                $match = 0;
                $name = undef;
            }
        }, errmsg => "drbdsetup error");
    };

    my $err = $@;
    die $err if $err;

    return $status;
}

sub drbd_resource {
    my ($resource) = @_;

    my $overview = drbd_overview($resource);
    die "DRBD resource $resource not defined in DRBD configuration"
        if (!$overview->{$resource});

    return $overview->{$resource};
}

sub drbd_admin_cmd {
    my ($command, $arg) = @_;

    my $cmd = ['/sbin/drbdadm', "$command", "$arg"];
    # warn "JJJJ: $command $arg";
    run_command($cmd, errmsg => "drbdadm error");

    my $err = $@;
    die $err if $err;
}

sub drbd_admin_up {
    my ($resource) = @_;

    drbd_admin_cmd('up', $resource);
}

sub drbd_admin_down {
    my ($resource) = @_;

    drbd_admin_cmd('down', $resource);
}

sub drbd_admin_primary {
    my ($resource) = @_;

    drbd_admin_cmd('primary', $resource);
}

sub drbd_admin_secondary {
    my ($resource) = @_;

    drbd_admin_cmd('secondary', $resource);
}

sub drbd_admin_adjust {
    my ($resource) = @_;

    drbd_admin_cmd('adjust', $resource);
}

sub get_vol_size {
    my ($scfg) = @_;

    my $size = 0;

    my $resource = $scfg->{resource};
    my $status = drbd_status($resource);
    if ($status) {
        $size = $status->{$resource}->{size};
    }

    return $size;
}

sub list_disks {
    my ($vmid) = @_;

    my $cfspath = PVE::QemuConfig->cfs_config_path($vmid);
    my $conf = PVE::Cluster::cfs_read_file($cfspath) || {};

    my $names = [];

    my $add_drive = sub {
        my ($volid, $is_cdrom) = @_;

        return if !$volid;
        return if $is_cdrom;

        push @$names, $volid;
    };

    PVE::QemuServer::foreach_drive($conf, sub {
        my ($ds, $drive) = @_;
        &$add_drive($drive->{file}, PVE::QemuServer::drive_is_cdrom($drive));
    });

    return $names;
}

sub all_disks {
    my ($class) = @_;

    my $list = PVE::QemuServer::vzlist();

    my $names = [];
    foreach my $vmid (keys %$list) {
        my $n = list_disks($vmid);
        push @$names, @$n;
    }
    return $names;
}

# Get the basename out of the configured raw device
sub get_name {
    my ($scfg) = @_;

    return $scfg->{resource};
}

# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m/vm-(\d+)-disk-\S+/) {
        return ('images', $volname, $1, undef, undef, undef, 'raw');
    } else {
        die "Invalid volume $volname must be *vm-<vmid>-disk-*";
    }
}

sub filesystem_path {
    my ($class, $scfg, $volname, $snapname) = @_;

    # warn "JJJJJJ f_path 1: $volname";

    die "snapshot is not implemented" if defined($snapname);

    my ($vtype, $nam, $vmid) = $class->parse_volname($volname);

    my $resource = drbd_resource($volname);
    my $drbdvol = $resource->{vol};

### keep this code for later use
###    # Depending on the configuration, we have different symlinks
###    # generated by the DRBD driver
###    my $path = "/dev/drbd/by-res/$volname/$drbdvol";
###    $path = "/dev/drbd/by-res/$volname" if ( ! -e $path );
###    die "DRBD device $path not found" if ( ! -e $path );

    # When booting and the VM is started immediatelly, Proxmox seems
    # to execute this before activating the storage. So the DRBD driver
    # is not initialized and the devices are not created.
    # This sequence have to be so (see PVE/QemuServer.pm(vm_start). We
    # could implement a new config parameter to know which variant we
    # need to expect.
    # We support currently only the new symlink format generated by the
    # DRBD driver and do no check for existance
    my $path = "/dev/drbd/by-res/$volname/$drbdvol";

    # warn "JJJJJJ f_path 2: $volname, $vmid, $path";

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "Creating base image is currently unimplemented";
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    die "can't clone images in this storage\n";
}

# We can't allocate something from the disk image, but we need to return
# a name to be used as volume name.
# Seems like this method gets size in kilobytes somehow,
# while listing methost return bytes. That's strange.
sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "unsupported format '$fmt'" if $fmt ne 'raw';

    # Convert kByte to bytes
    $size *= 1024;

    my $sz = get_vol_size($scfg);
    die "Disk size '$size' to big (max: '$sz')\n"
        if $size > $sz;

    die "illegal name '$name' - sould be 'vm-$vmid-*'\n"
        if $name && $name !~ m/^vm-$vmid-/;

    $name = get_name($scfg) if !$name;
    my ($vtype, $n, $id) = $class->parse_volname($name);

    die "This storage is reserved for VM-ID '$id'\n"
        if $id !=  $vmid;

    #my $all_disks = $class->all_disks();
    #my %disks = map { $_ => 1 } @$all_disks;
    #die "Disk '$name' already in use\n"
    #    if(exists($disks{$name}));

    # warn "JJJJJJ alloc_image: $storeid, $vmid, $size, $name";

    # this is the volume name used in all other functions as "$volname"
    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $name = get_name($scfg);
    my $sz = get_vol_size($scfg);
    my ($vtype, $n, $id) = $class->parse_volname($name);
    my $volid = "$storeid:$name";

    my $skip = 0;

    if ($vollist) {
        #warn "JJJJJJ list_images vl: $vollist";
        my $found = grep { $_ eq $volid } @$vollist;
        $skip = 1 if !$found;
    } else {
        #warn "JJJJJJ list_images vm: $vmid";
        $skip = 1 if defined($vmid) && ($id ne $vmid);
    }

    my $res = [];
    if ($skip == 0) {
        push @$res, {
            'volid' => $volid, 'format' => 'raw', 'size' => $sz, vmid => $id,
        };
    }

    #warn "JJJJJJ list_images: $storeid, $vmid, $vollist, $id, $name";

    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{size} = get_vol_size($scfg) if !$cache->{size};

    my $total = $cache->{size};
    my $free = 0;
    my $used = 0;
    my $exists = 0;

    my ($vt, $nam, $vmid) = $class->parse_volname($storeid);
    my $names = list_disks($vmid);

    foreach my $n (@$names) {
        my ($stor, $volid) = split(':', $n);
        $exists = 1 if ( $storeid eq $stor );
    }

    # disk in use -> whole store is used up; else whole is free
    if ($exists) {
        $used = $total;
    } else {
        $free = $total;
    }

    #my $name = get_name($scfg);
    #warn "JJJJJJ status: $storeid, $vmid, st:$nam, $name, t:$total, f:$free, u:$used";

    return [$total, $free, $used, 1];
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    # warn "JJJJJJ activate_storage 1: $storeid";

    my $volname = $scfg->{resource};

    # just to check if configured name is correct
    $class->parse_volname($volname);

    my $resource = drbd_resource($volname);
    if ($resource->{connect} == STANDALONE) {
        drbd_admin_adjust($volname);
    }

    $resource = drbd_resource($volname);
    if ($resource->{connect} == UNCONFIGURED) {
        drbd_admin_up($volname);
        drbd_admin_secondary($volname);
    }

    # warn "JJJJJJ activate_storage 2: $storeid";

    return 1;
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $volname = $scfg->{resource};
    my $resource = drbd_resource($volname);
    if ($resource->{connect} != UNCONFIGURED) {
        drbd_admin_secondary($volname);
        drbd_admin_down($volname);
    }

    # warn "JJJJJJ deactivate_storage: $storeid";

    return 1;
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;
  
    # warn "JJJJJJ activate_volume 1: $storeid, $volname";

    my $resource = drbd_resource($volname);
    die "DRBD resource $volname not up"
        if ($resource->{connect} == UNCONFIGURED);
    die "DRBD resource $volname peer is primary"
        if ($resource->{prole} == PRIMARY);

    drbd_admin_primary($volname);

    # warn "JJJJJJ activate_volume 2: $storeid, $volname";

    return 1;
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    my $resource = drbd_resource($volname);
    die "DRBD resource $volname not up"
        if ($resource->{connect} == UNCONFIGURED);

    drbd_admin_secondary($volname);

    # warn "JJJJJJ deactivate_volume: $storeid, $volname";

    return 1;
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    die "resize is not implemented";
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "snapshot is not implemented";
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "snapshot rollback is not implemented";
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "snapshot delete is not implemented";
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    # warn "JJJJJJ volume_has_feature: $storeid";

    my $features = {
	copy => { base => 1, current => 1},
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
        $class->parse_volname($volname);

    my $key = undef;
    if ($snapname){
	    $key = 'snap';
    } else {
	    $key =  $isBase ? 'base' : 'current';
    }
    return 1 if $features->{$feature}->{$key};

    return undef;
}

1;
