pyLibrary
=========

A library of wonderful Python things!

Motivation
----------

This library is born from my version of the `utils` library everyone makes.
Only, instead of being utilities that are specific to the task, these utilities
are for programming in general: They assume logs should be structured,
all data should be JSONizable, and OO is preferred, and more.

### Python is a Little Crufty ###

Python is awesome now, but it was originally a procedural language invented
before pure functional semantics, before OO, and even before the
discovery of vowels.  As a consequence there are many procedures that alter
their own parameters, or have disemvoweled names.  This library puts a facade
over these relics of the past and uses convention to name methods.

Installing pyLibrary
--------------------

Python packages are easy to install, assuming you have Python (see below).

    pip install pyLibrary

Installing for Development
--------------------------

  * Download from Github:

        git clone https://github.com/klahnakoski/pyLibrary.git

  * Install requirements:

        python setup.py develop


Windows 7 Install Instructions for Python
-----------------------------------------

Updated November 2014, for Python 2.7.8

Python was really made for Linux, and installation will be easier there.
Technically, Python works on Windows too, but there are a few gotchas you can
avoid by following these instructions.

  * Download Python 2.7
    * 32bit ONLY!!! Many native libs are 32 bit
    * Varsion 2.7.8 or higher (includes pip, so install is easier)
  * Install Python at ```c:\Python27``` (The space in the "Program Files" may screw up installs of native libs)
  * Add to you path: ```c:\Python27;c:\Python27\scripts;```
  * Download ```https://bootstrap.pypa.io/get-pip.py```

        CALL python get-pip.py
        CALL pip install virtualenv

  * Many "Python Powered" native installs require a pointer to the python installation, but they have no idea where to
  look in 64bit windows.  You must alter the registry ([http://stackoverflow.com/questions/3652625/installing-setuptools-on-64-bit-windows](http://stackoverflow.com/questions/3652625/installing-setuptools-on-64-bit-windows)):

        SET HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\Python\PythonCore\2.7\InstallPath = "C:\Python27"

###Using virtualenv

```virtualenv``` allows you to have multiple python projects on the same
machine, even if they use different versions of the same libraries.
```virtualenv``` does this by making a copy of the main python directory and
using it to hold the specific versions required.

* New environment: ```virtualenv <name_of_dir>```
* Activate environment: ```<name_of_dir>\scripts\activate```
* Exit environment: ```deactivate```

If you have more than one project on your dev box I suggest you do all your
work inside a virtual environment.

### PyPy and Virtual Environments

```virtualenv``` can be used with PyPy, but it is a bit more involved.  The
paths must be explict, and some copying is required.

#### New environment:
The first call to virtualenv will make the directory, to which you copy the
PyPy core libraries, and the second call finishes the install.

    c:\PyPy27\bin\virtualenv <name_of_dir>
    copy c:\PyPy27\bin\lib_pypy <name_of_dir>
    copy c:\PyPy27\bin\lib_python <name_of_dir>
    c:\PyPy27\bin\virtualenv <name_of_dir>

#### Activate environment:
With CPython ```virtualenv``` places it's executables in ```Scripts```.  The
PyPy version uses ```bin```

    <name_of_dir>\bin\activate

#### Using PIP in PyPy:

PyPy does not share any libraries with CPython.  You must install the PyPy libraries using 

	C:\pypy\bin\pip.exe

The `pip` found in your `%PATH%` probably points to `C:\python27\Scripts\pip.exe`.

#### Using PIP in PyPy virtualenv:

Do **NOT** use the ```<name_of_dir>\Scripts``` directory: It installs to your
main PyPy installation.  Pip install is done using the `bin` directory:

    <name_of_dir>\bin\pip.exe

#### Exit environment:
Deactivation is like normal

    deactivate

### CPython Binaries and Virtual Environments

If you plan to use any binary packages, ```virtualenv``` will not work
directly.  Instead, install the binary (32 bit only!!) to the main python
installation.  Then copy any newly installed files/directories from
```C:\Python27\Lib\site-packages``` to ```<name_of_dir>\Lib\site-packages```.

### Binaries and PyPy

This strategy for installing binaries into Virtual Environments is almost
identical to installing binaries into your PyPy environment: Install Numpy
and Scipy to your CPython installation using a windows installer (which has
pre-compiled binaries), and then copy the ```C:\Python27\Lib\site-packages\<package>```
to ```c:\PyPy\site-packages\```; note lack of ```Lib``` subdirectory.

