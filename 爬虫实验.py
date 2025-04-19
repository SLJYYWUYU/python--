import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
import time
import csv
import os
import datetime
import threading

# 全局配置
BASE_URL = "http://paper.people.com.cn/rmrb/html"
ROBOTS_TXT = "http://paper.people.com.cn/robots.txt"
OUTPUT_FILE = "rmrb_data.csv"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
TARGET_URLS_COUNT = 20  # 要爬取的URL数量

def get_robots_info():
    """解析robots.txt文件"""
    print("\n=== 解析robots.txt ===")
    rp = RobotFileParser()
    rp.set_url(ROBOTS_TXT)
    try:
        rp.read()
        print(f"可以爬取: {'可以' if rp.can_fetch('*', BASE_URL) else '不可'}")
        print(f"爬取延迟: {rp.crawl_delay('*') or '无限制'}")
        return rp
    except Exception as e:
        print(f"解析robots.txt失败: {e}")
        return None

def fetch_url(url):
    """获取网页内容"""
    try:
        headers = {'User-Agent': USER_AGENT}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = 'utf-8'
        return response.text
    except Exception as e:
        print(f"获取 {url} 失败: {e}")
        return None

def extract_target_urls(target_date):
    """解析目标日期，提取20个待爬取URL"""
    date_str = target_date.strftime("%Y-%m/%d")
    index_url = f"{BASE_URL}/{date_str}/nbs.D110000renmrb_01.htm"
    print(f"\n正在解析首页获取URL: {index_url}")
    
    html = fetch_url(index_url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    target_urls = []
    
    # 1. 获取所有版面链接
    page_links = []
    page_list = soup.find('div', {'id': 'pageList'}) or soup.find('div', {'class': 'swiper-container'})
    if page_list:
        page_links = [a['href'] for a in page_list.find_all('a', href=True)]
    
    print(f"找到 {len(page_links)} 个版面")
    
    # 2. 从各版面收集文章URL (直到凑够20个)
    for i, page_link in enumerate(page_links):
        if len(target_urls) >= TARGET_URLS_COUNT:
            break
            
        page_url = f"{BASE_URL}/{date_str}/{page_link}"
        print(f"解析版面 {i+1}: {page_url}")
        
        page_html = fetch_url(page_url)
        if not page_html:
            continue
            
        page_soup = BeautifulSoup(page_html, 'html.parser')
        
        # 获取文章链接
        article_list = page_soup.find('div', {'id': 'titleList'}) or page_soup.find('ul', {'class': 'news-list'})
        if article_list:
            article_links = [f"{BASE_URL}/{date_str}/{a['href']}" 
                           for a in article_list.find_all('a', href=True)]
            target_urls.extend(article_links)
            
            print(f"已收集 {len(target_urls)}/{TARGET_URLS_COUNT} 个URL")
            if len(target_urls) >= TARGET_URLS_COUNT:
                break
    
    return target_urls[:TARGET_URLS_COUNT]

def extract_article_info(url, html):
    """从文章页面提取信息"""
    soup = BeautifulSoup(html, 'html.parser')
    
    # 提取标题
    title = ''
    title_tag = soup.find('h1') or soup.find('div', {'class': 'article-title'})
    if title_tag:
        title = title_tag.get_text(strip=True)
    
    # 提取日期
    date = ''
    date_tag = soup.find('time') or soup.find('div', {'class': 'date'})
    if date_tag:
        date = date_tag.get_text(strip=True)
    
    # 提取内容
    content = ''
    content_div = soup.find('div', {'id': 'ozoom'}) or soup.find('div', {'class': 'article-content'})
    if content_div:
        content = '\n'.join(p.get_text(strip=True) for p in content_div.find_all('p'))
    
    return {
        'url': url,
        'title': title,
        'date': date,
        'content': content,
        'content_length': len(content)
    }

def save_to_csv(data, filename):
    """保存数据到CSV"""
    file_exists = os.path.isfile(filename)
    
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

def single_thread_crawl(url_list):
    """单线程爬取（保留你想要的单线程功能）"""
    print("\n=== 单线程爬取开始 ===")
    start_time = time.time()
    
    for i, url in enumerate(url_list, 1):
        print(f"进度: {i}/{len(url_list)} - {url}")
        html = fetch_url(url)
        if html:
            data = extract_article_info(url, html)
            save_to_csv(data, OUTPUT_FILE)
        time.sleep(1)  # 请求间隔
    
    print(f"单线程完成! 耗时: {time.time()-start_time:.2f}秒")

def worker(url_list, thread_name):
    """多线程工作函数"""
    for i, url in enumerate(url_list, 1):
        print(f"{thread_name} 进度: {i}/{len(url_list)} - {url}")
        html = fetch_url(url)
        if html:
            data = extract_article_info(url, html)
            save_to_csv(data, OUTPUT_FILE)
        time.sleep(1)  # 请求间隔

def multi_thread_crawl(url_list):
    """双线程爬取"""
    print("\n=== 双线程爬取开始 ===")
    start_time = time.time()
    
    # 将URL列表分成两部分
    mid = len(url_list) // 2
    thread1 = threading.Thread(target=worker, args=(url_list[:mid], "线程1"))
    thread2 = threading.Thread(target=worker, args=(url_list[mid:], "线程2"))
    
    thread1.start()
    thread2.start()
    
    thread1.join()
    thread2.join()
    
    print(f"双线程完成! 耗时: {time.time()-start_time:.2f}秒")

def main():
    # 检查robots.txt
    rp = get_robots_info()
    if rp and not rp.can_fetch('*', BASE_URL):
        print("根据robots.txt，不允许爬取此网站")
        return
   
    # 使用已知有效的日期进行测试
    target_date = datetime.datetime(2023, 1, 1)
    print(f"\n准备爬取 {target_date.strftime('%Y-%m-%d')} 的人民日报")
    
    # 解析出20个待爬取URL
    target_urls = extract_target_urls(target_date)
    if not target_urls:
        print("无法获取目标链接，请检查日期或网站结构")
        return
    
    print("\n成功获取的20个URL列表:")
    for i, url in enumerate(target_urls, 1):
        print(f"{i}. {url}")
    
    # 清空输出文件
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    
    # 单线程爬取
    single_thread_crawl(target_urls)
    
    # 清空文件准备多线程测试（实际使用时可以注释掉）
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    
    # 双线程爬取
    multi_thread_crawl(target_urls)
    
    print("\n=== 所有任务完成 ===")

if __name__ == '__main__':
    main()
    