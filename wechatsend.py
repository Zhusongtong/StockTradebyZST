import requests
import os
from datetime import datetime

def send_log_to_wechat(log_file_path, sendkey, title="日志推送"):
    """
    读取日志文件并推送到微信
    
    Args:
        log_file_path (str): 日志文件路径
        sendkey (str): Server酱的SendKey
        title (str): 推送标题
    """
    
    # 检查日志文件是否存在
    if not os.path.exists(log_file_path):
        print(f"❌ 日志文件不存在: {log_file_path}")
        return False
    
    try:
        # 读取日志文件内容
        with open(log_file_path, 'r', encoding='utf-8') as file:
            log_content = file.read().strip()
        
        if not log_content:
            print("❌ 日志文件为空")
            return False
        
        print("📄 日志文件内容预览:")
        print(log_content[:200] + "..." if len(log_content) > 200 else log_content)
        print("-" * 50)
        
        # 构建推送内容
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        content = f"**推送时间**: {current_time}\n\n**日志内容**:\n\n```\n{log_content}\n```"
        
        # Server酱推送配置
        server_url = f"https://sctapi.ftqq.com/{sendkey}.send"
        
        # 推送数据
        data = {
            "title": title,
            "desp": content
        }
        
        # 发送请求
        print("📤 正在推送到微信...")
        response = requests.post(server_url, data=data)
        
        # 检查推送结果
        if response.status_code == 200:
            response_data = response.json()
            if response_data.get('code') == 0:
                print("✅ 消息已成功推送到微信！")
                return True
            else:
                print(f"❌ 推送失败: {response_data.get('message', '未知错误')}")
                return False
        else:
            print(f"❌ HTTP请求失败，状态码: {response.status_code}")
            print("错误信息:", response.text)
            return False
            
    except FileNotFoundError:
        print(f"❌ 找不到日志文件: {log_file_path}")
        return False
    except UnicodeDecodeError:
        print("❌ 日志文件编码错误，尝试使用其他编码")
        try:
            with open(log_file_path, 'r', encoding='gbk') as file:
                log_content = file.read().strip()
            return send_log_content(log_content, sendkey, title)
        except:
            print("❌ 无法读取日志文件，请检查文件编码")
            return False
    except Exception as e:
        print(f"❌ 发生错误: {str(e)}")
        return False

def send_log_content(log_content, sendkey, title="日志推送"):
    """
    直接推送日志内容到微信
    
    Args:
        log_content (str): 日志内容
        sendkey (str): Server酱的SendKey
        title (str): 推送标题
    """
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        content = f"**推送时间**: {current_time}\n\n**日志内容**:\n\n```\n{log_content}\n```"
        
        server_url = f"https://sctapi.ftqq.com/{sendkey}.send"
        data = {
            "title": title,
            "desp": content
        }
        
        response = requests.post(server_url, data=data)
        
        if response.status_code == 200:
            response_data = response.json()
            if response_data.get('code') == 0:
                print("✅ 消息已成功推送到微信！")
                return True
            else:
                print(f"❌ 推送失败: {response_data.get('message', '未知错误')}")
                return False
        else:
            print(f"❌ HTTP请求失败，状态码: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ 发生错误: {str(e)}")
        return False

# ============================
# 主程序执行部分
# ============================

if __name__ == "__main__":
    # 配置参数
    LOG_FILE_PATH = "select_results.log"  # 日志文件路径
    SENDKEY = "SCT270757TGYSsmGdzCUzGJAgPDN8kaH1E"  # 你的 SendKey
    PUSH_TITLE = "筛选结果日志"  # 推送标题
    
    # 执行推送
    success = send_log_to_wechat(LOG_FILE_PATH, SENDKEY, PUSH_TITLE)
    
    if success:
        print("🎉 任务完成！")
    else:
        print("💔 任务失败，请检查配置和网络连接")