import argparse
import os 
import subprocess

def build_spark_submit(kafka_type, mode):
    if kafka_type.lower() in ["producer", "consumer"]:
        kafka_type = kafka_type.lower().capitalize()

    else:
        raise Exception("type needs to be 'producer' or 'consumer'")
    

    script_dir = os.path.dirname(os.path.abspath(__file__))

    if mode.lower() == "local":
        deploy_mode = "local[*]"
    else:
        pass

    module_file = (
        script_dir + "/KafkaConfig.py" +
        script_dir + "/helpers.py"
    )
    
    command = "spark-submit" + \
              " --name " + "Kafka " + kafka_type + \
              " --master " + deploy_mode + \
              " --py-files " + " ".join(module_file) + \
              script_dir + "/" + kafka_type + ".py"

    return command

def execute_shell_cmd(cmd):
    print(cmd)
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print("Command output:", proc.stdout)
        print("Return code:", proc.returncode)
        for line in proc.stdout:
            print(line)
        exec_flg = proc.wait()
        return exec_flg
    except Exception as e:
        # log e
        print(e)

def main(kafka_type, mode):
    spark_submit_cmd = build_spark_submit(kafka_type, mode)
    flag = execute_shell_cmd(spark_submit_cmd)
    print(flag)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inbound jobs for bank data application - producer/consumer")
    parser.add_argument('arg1', type=str, help="producer or consumer")
    parser.add_argument('arg2', type=str, help="deploy mode")
    args = parser.parse_args()
    main(args.arg1, args.arg2)