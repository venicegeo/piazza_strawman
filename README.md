# Piazza Strawman

This is an exploration of some of the technical ideas that will go into the Piazza project.
The key concept is messaging queues as a technique for decoupling ingestion of data from processing.
We provide implementations of some web services and data processing facilities based around this idea, and scripts to deploy them on a virtual network.

## Setup
We use a number of third-party components for this project which are insuitable for inclusion in the git repository, but a script is provided to help download them.
The script will also compile the code under the services/ directory and make it available for the virtual machine deployment scripts.
To run the setup script and the virtual network, you will need:

   * VirtualBox http://virtualbox.org/
   * Vagrant http://vagrantup.com/
   * Ansible http://ansible.com/
   * OpenSSH http://openssh.com/
   * Wget http://www.gnu.org/s/wget/
   * Java http://www.oracle.com/technetwork/java/javase/downloads/index.html

This script is tested on Mac OSX but should work on Linux as well with these tools installed.
To run, change directories to the root of the checked-out project and type `sh setup.sh`.

This will download a lot of data and likely take a while the first time it runs, subsequent runs should be faster.

After setup.sh is done, you can instantiate the virtual network by changing directories to the `messaging` directory and running `vagrant up`.
Vagrant may need to download a virtual machine image so again this may take a while.
Also you'll need about 4GB of free space for the virtual machines to run.

Once Vagrant finishes, you should be able to visit http://192.168.23.11:8080/ in your web browser and see a simple web page that exercises the data processing pipeline.

## Vagrant notes

The following are some common useful commands for working with the vagrant-managed machines.
Most commands accept a machine name (`geoserver`, `database`, or `messaging`) to target only a single machine from the virtual network.
If the name is omitted, some commands will apply to all machines and others will fail.

* `vagrant up` creates a machine if it doesn't already exist, or starts it if it exists but it stopped.

* `vagrant ssh` opens an SSH shell session with the specified machine.
  This uses the `vagrant` user which has permission to sudo without a password.

* `vagrant rsync` copies files from the messaging/ directory to the vagrant user's ~/sync/ directory on the virtual machine.

* `vagrant destroy` completely erases the virtual machine.

* `vagrant halt` shuts down the virtual machine but preserves its virtual hard drive.

* `vagrant provision` re-runs the setup scripts without erasing anything.
   The ansible scripts we use are intended to support this type of usage.
