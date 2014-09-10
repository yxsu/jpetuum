Java version of Petuum
===============
# Installation
The Java version of Petuum works without any problems on Linux/Windows/MacOSX as long as the newest Java Development Kit(JDK) is installed correctly. 

## Install JDK1.8
As there are many new language features and libraries used in our Java implementation of Petuum, the whole system must be compiled and executed only with JDK1.8 or higher, which can be downloaded from the Oracle website(http://www.oracle.com/technetwork/java/javase/downloads/index.html). For Windows and Mac, there is no problem about installing JDK1.8, but for Ubuntu 14.04 or lower, there is no JDK1.8 support in its software repository. So, you must configure the JAVA_HOME and PATH manually.

## Install Gradle
We use Gradle as the automation tool for dependency managment and compiling toolkit in this system. So, in order to compile and run the system correctly, you must install Gradle manually. The minimum version of Gradle we support is 1.11. Thus, if you are an Ubuntuer, do not install Gradle as "sudo apt-get install gradle", because the version of Gradle in Ubuntu repository is older than that we required. Please go to the official website(http://www.gradle.org/) to download and install the newest version of Gradle.

# Editing and Compiling

Once the code of whole system is downloaded, you can enter to the project directory and type the following command to compile and run the default demo application about matrix factorization.

``` sh
gradle run
```

This command will download some extra packages which our system uses and then compile the Petuum system and the related demo applications. Thus, this command will take minutes when you first run this command.

If you want to write more applications, please create an exectuable Java class in com.petuum.app like the default demo MatrixFact. If you want to change the default demo application which will be executed with "gradle run", please modify the build.gradle file to change the "mainClassName" to yours. 

