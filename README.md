# NaiadOnYarn

## Intro

A small project to run Naiad on yarn, which is to launch each naiad process in one yarn container. 

## Usage
1. Build the project: `mvn package`
2. Run the naiad program you want with container requirement and naiad program options.

Here is an example:
```bash
yarn jar ./target/NaiadOnYarn-1.0-SNAPSHOT.jar NaiadYarnClient\
 -app_name NaiadProgram\
 -container_memory 512\
 -container_vcores 1\
 -master_memory 8\
 -app_priority 1\
 -program 'mono NaiadProgram.exe arg1 arg2'\
 -num_process 3\
 -hosts worker1:1,worker2:2\
 -num_thread 1\
 -port 2100\
 -log_dir /data/to/directory
```

1. By only running `yarn jar ./target/NaiadOnYarn-1.0-SNAPSHOT.jar NaiadYarnClient` will show all the available arguments. 
2. `app_name` tells the name of the yarn application;
3. `conatiner_memory`, `container_vcores` and `app_priority` specify 
the memory (in GB), number of virtual cores and priority of one worker container.
One worker container contains only one naiad process.
4. `master_memory` specifies the memory (in GB) required by the application master
who just simply manages the worker containers. Because naiad doesn't have master, 
usually we don't need to give too much memory to the application master.
5. `program` tells the naiad program we want to run with the arguments that
will be passed to the program. **Please use `'` instead of `"` here**.
6. `num_process` tells how many naiad processes are there. This equals to
the number of worker containers that will be created.

    If you want to manage location of the naiad processes, you need to tell clearly
by `hosts`. The format of `hosts` is `host1:num1,host2:num2,host3:num3,...`, which means there will be `num1` naiad processes on `host1`, `num2` processes on `host2`, etc. Different hosts are split
by comma and no space is allowed. If you also specify `num_process`,
make sure the number of processes that will be created in `hosts` equals to `num_process`.

    At least one of `num_process` or `hosts` should be specified. If only `hosts` is given, `num_process` will be updated automatically. If only `num_process` is given, application master will request `num_process` containers first and update `hosts` accordingly.
7. `num_thread` tells the number of threads that will be created in each naiad process. This is optional. Default to 1.
8. `port` tells the port that a naiad process will listen on. If there are more
than one processes that will created in the same host, the latter process will increase
the port by 1 automatically. This is optional. Default to 2100.
9. `log_dir` tells the directory that the logs of worker containers, master application will be written to. 
This is optional. If not given, the logs will be located at what is specified in yarn configuration.

