#!/usr/bin/env python3

import threading
import subprocess
import os
import sys
import time
import signal

def run_pmwatch(output_file, stop_event):
    """运行 pmwatch 命令，直到 stop_event 被设置"""
    try:
        cmd = ["sudo", "pmwatch", "1", "-F", output_file, "-t"]
        print(f"启动 pmwatch: {' '.join(cmd)}")
        with open(output_file, "a") as f:
            process = subprocess.Popen(cmd, stdout=f, stderr=subprocess.PIPE, text=True, preexec_fn=os.setsid)
            while not stop_event.is_set():
                if process.poll() is not None:
                    break
                time.sleep(0.1)
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            _, stderr = process.communicate()
            if process.returncode != 0:
                print(f"pmwatch 错误: {stderr}")
            else:
                print("pmwatch 成功终止")
    except Exception as e:
        print(f"pmwatch 线程异常: {e}")

def run_traffic_script(script_path, output_file, stop_event):
    """运行 ex3.sh 脚本，捕获输出并写入 output_file，完成后设置 stop_event"""
    try:
        cmd = ["bash", script_path]
        print(f"启动脚本: {' '.join(cmd)}")
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        # 打开输出文件，追加模式
        with open(output_file, "a") as f:
            # 逐行读取 ex3.sh 的输出并写入
            for line in process.stdout:
                f.write(f"[ex3.sh] {line}")
                f.flush()  # 确保实时写入
            process.stdout.close()
            
            # 等待进程结束
            _, stderr = process.communicate()
            if process.returncode != 0:
                print(f"ex3.sh 错误: {stderr}")
            else:
                print("ex3.sh 成功完成")
        stop_event.set()
    except Exception as e:
        print(f"ex3.sh 线程异常: {e}")
        stop_event.set()

def main():
    # 定义文件路径
    output_file = "/home/scae/sc/data/ex3/Meto.csv"
    script_path = "ex3.sh"

    # 检查脚本是否存在
    if not os.path.exists(script_path):
        print(f"错误: {script_path} 不存在")
        sys.exit(1)

    # 检查输出目录是否存在
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"创建输出目录: {output_dir}")

    # 创建 Event 对象
    stop_event = threading.Event()

    # 创建线程
    pmwatch_thread = threading.Thread(target=run_pmwatch, args=(output_file, stop_event))
    traffic_thread = threading.Thread(target=run_traffic_script, args=(script_path, output_file, stop_event))

    # 启动线程
    pmwatch_thread.start()
    time.sleep(1)  # 确保 pmwatch 先启动
    traffic_thread.start()

    # 等待线程完成
    traffic_thread.join()
    time.sleep(5)
    pmwatch_thread.join()

    print("所有线程已完成")

if __name__ == "__main__":
    main()