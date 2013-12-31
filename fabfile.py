from fabric.api import local, env, settings, execute, task
from fabric.decorators import runs_once
from fabric.operations import put, prompt, run, sudo
from fabric.context_managers import shell_env

def get_cluster_name():
  return "%(cluster_prefix)s-%(cluster_size)s" % env

def get_hdfs_root():
  return "hdfs://%s:9000" % (get_master_hostname(),)

def get_hdfs_bam():
  return get_hdfs_root() + env.bam_file_hdfs

def get_master_hostname():
  key="master_hostname"
  if not key in env:
    env[key] = prompt("What is the master's hostname?")
  return env[key]

@task
@runs_once
def launch_cluster():
  local("%(spark_ec2_script)s -k %(key_name)s -i %(key_filename)s -s %(cluster_size)s " \
        "--zone %(cluster_zone)s --instance-type %(cluster_instance_type)s -v %(spark_version)s " \
        "launch " % env + get_cluster_name())

@task
@runs_once
def destroy_cluster():
  local("%(spark_ec2_script)s destroy " % env + get_cluster_name())

def __copy_jarfile():
  # Put the file on the master
  put(env.adam_jar, env.adam_jar)
  # Distribute it to the workers
  run("sudo /root/spark-ec2/copy-dir " + env.adam_jar)

@task
@runs_once
def copy_jarfile():
  master_hostname = get_master_hostname()
  execute(__copy_jarfile, hosts=[master_hostname])

def __distcp(master_hostname):
  # Start M/R
  run("sudo /root/ephemeral-hdfs/bin/start-mapred.sh")
  # Run distcp to copy file from s3 to hdfs
  run("sudo /root/ephemeral-hdfs/bin/hadoop distcp " \
     "-Dfs.s3n.awsAccessKeyId='%(aws_access_key_id)s' " \
     "-Dfs.s3n.awsSecretAccessKey='%(aws_secret_access_key)s' " \
     "%(bam_file_s3)s " % env \
     + get_hdfs_bam())
  # Stop M/R
  run("sudo /root/ephemeral-hdfs/bin/start-mapred.sh")

@task
@runs_once
def distcp():
  master_hostname = get_master_hostname()
  # Run this task on the master node only
  execute(__distcp, master_hostname, hosts=[master_hostname])

@task
def sort_markdup():
  # Fetch the BAM file
  distcp()
  # Copy the ADAM jar file to the master
  copy_jarfile()
  # Set parallelism to 2x the number of CPUs
  parallelism = int(env.cluster_size) * int(env.cluster_cpus_per_node) * 2
  # Run ADAM
  with shell_env(SPARK_MEM="%(spark_mem)s" % env, SPARK_JAVA_OPTS=" -Dspark.default.parallelism=%s" % (parallelism,)):
    run("time java $SPARK_JAVA_OPTS -Xmx16g -jar $adam_jar transform -mark_duplicate_reads -sort_reads" \
              "-spark_master spark://" + get_master_hostname() + ":7077" \
              "-spark_home /root/spark " \
              "-spark_jar " + env.adam_jar + " " \
              + get_hdfs_bam() + " " \
              + get_hdfs_bam() + ".sortmarkdup")

@task
def runtest():
  launch_cluster()
  sort_markdup()
  destroy_cluster()
