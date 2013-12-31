# ADAM performance test

Launches a Spark cluster on EC2, copies over the ADAM jar
and then run performance tests.

You will need to install [Fabric](http://fabfile.org) to run this
test.

Copy the `fabricrc.template` file to `fabricrc` and edit it.

To run the test,
```
$ fab -c fabricrc runtest
```

To see a list of available tasks,
```
$ fab -l
```

