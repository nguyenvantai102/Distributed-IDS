<h1 align='center'>✨ONOS Controller✨ </h1>

<h2 align="center">🛠 Install & Set up ONOS 🛠</h2>
- Install Java 8

```
sudo apt install openjdk-8-jdk
sudo su
cat >> /etc/environment <<EOL
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
JRE_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre
EOL
```
- Install Git and Maven


`sudo apt-get install git maven`

- Add User SDN
```
sudo adduser sdn -–system –group
sudo passwd sdn
```

** NOTE: Set password to “rocks”**

- Install ONOS (version 1.12)

```
git clone http://gerrit.onosproject.org/onos -b onos-1.12
cd ~/onos
source tools/dev/bash_profile
onos-buck build onos
```
Edit .profile file:

`vi ~/.profile`
**
export ONOS_ROOT=~/onos
source $ONOS_ROOT/tools/dev/bash_profile
**

<h2 align="center">🔥 Launching ONOS 🔥</h2>
- Terminal
```
cd ~/onos
onos-buck run onos-local
```

- ONOS Browser

  **http://localhost<ip-onos-vm>:8181/onos/ui**
 
  username: onos
 
  passwork: rocks

<h2 align="center">🌱 Result (with Mininet) 🌱</h2>


<p align="center"> <img src="https://user-images.githubusercontent.com/67199007/178035669-807dbf66-bd47-4c8f-9ebb-027bfefebea0.png"></p>
