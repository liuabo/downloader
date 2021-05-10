import urllib3,os
from pathlib import Path
from urllib import parse
from concurrent.futures import ThreadPoolExecutor

import requests
from tqdm import tqdm
from faker import Faker
from retry import retry
from Crypto.Cipher import AES


urllib3.disable_warnings()


class Downloader():
    def __init__(self, url, dst=None, filename=None):
        """
        :param url: m3u8 文件下载地址
        :param dst: 指定下载视频文件输出目录，不指定则为当前目录
        :param filename: 下载视频文件名
        """
        self.url = url
        self.key = ''
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36'
        }
        self.dst = dst or os.getcwd()
        self.filename = filename or 'output.mp4'

        # ts 文件缓存目录
        self.tmp_folder = 'temp'

        self.session = requests.Session()
        self.session.headers.update({'User-Agent': Faker().user_agent()})
        self.session.verify = False

        self.proxies = {}

    def parse_m3u8_url(self):
        """
        获取m3u8文件 并解析文件获取ts视频文件地址
        :return: ts文件下载地址
        """
        # text = self.session.get(self.url).text
        #
        # return [parse.urljoin(self.url, row.strip()) for row in text.split('\n') if not row.startswith('#')]
        base_url = url[:url.rfind('/') + 1]  # 如果需要拼接url,则启用 , +1 把 / 加上
        rs = requests.get(url, headers=self.header).text
        list_content = rs.split('\n')
        player_list = []
        # 如果没有merge文件夹则新建merge文件夹，用于存放ts文件
        if not os.path.exists('merge'):
            os.system('mkdir merge')
        for index, line in enumerate(list_content):
            # 判断视频是否经过AES-128加密
            if "#EXT-X-KEY" in line:
                method_pos = line.find("METHOD")
                comma_pos = line.find(",")
                method = line[method_pos:comma_pos].split('=')[1]  # 获取加密方式
                print("Decode Method：", method)
                uri_pos = line.find("URI")
                quotation_mark_pos = line.rfind('"')
                key_path = line[uri_pos:quotation_mark_pos].split('"')[1]
                # key_path = 'https://vip5.bobolj.com/20210428/SNQTsAxY/800kb/hls/key.key'
                key_url = key_path
                res = requests.get(key_url)
                self.key = res.content  # 获取加密密钥
                print("self.key：", self.key)
            # 以下拼接方式可能会根据自己的需求进行改动
            if '#EXTINF' in line:
                # href = ''
                # 如果加密，直接提取每一级的.ts文件链接地址
                if 'http' in list_content[index + 1]:
                    href = list_content[index + 1]
                    player_list.append(href)
                # 如果没有加密，构造出url链接
                elif ('ad0.ts' not in list_content[index + 1]):
                    href = base_url + list_content[index + 1]
                    player_list.append(href)
        return player_list

    def check_save_folder(self):
        """
        检测视频输出目录是否正确，并创建temp目录用于临时存储ts文件
        :return: ts文件保存目录 (Path对象)
        """
        dst_folder = Path(self.dst)
        if not dst_folder.is_dir():
            raise Exception(f'{self.dst} is not a dir!')

        # 如果temp目录不存在便创建
        save_folder = Path(self.tmp_folder)
        if not save_folder.exists():
            save_folder.mkdir()

        return save_folder

    def download(self, ts_url, save_folder, pbar):
        """
        根据ts文件地址下载视频文件并保存到指定目录
        * 当前处理递归下载！！！
        :param ts_url: ts文件下载地址
        :param save_folder: ts文件保存目录
        :return: ts文件保存路径
        """
        count = 0
        try:
            # ts_url 可能有参数
            filename = parse.urlparse(ts_url).path.split('/')[-1]

            filepath = save_folder / filename
            res = requests.get(ts_url, headers=self.header)
            if not (200 <= res.status_code < 400):
                print(f'{ts_url}, status_code: {res.status_code}')
                raise Exception('Bad request!')
            if filepath.exists():
                # 文件已存在 跳过
                pbar.update(1)
                return str(filepath)

            if (len(self.key)):
                cryptor = AES.new(self.key, AES.MODE_CBC, self.key)
                with filepath.open('wb') as fp:
                    fp.write(cryptor.decrypt(res.content * 16))
            else:
                with filepath.open('wb') as fp:
                    fp.write(res.content)
            # print('下载完成')

        except Exception as e:
            print(e)
            return self.download(ts_url, save_folder, pbar)

        pbar.update(1)
        return str(filepath)

    def merge(self, ts_file_paths):
        """
        ts文件合成
        ffmpeg -i "concat:file01.ts|file02.ts|file03.ts" -acodec copy -vcodec copy output.mp4
        ffmpeg -f concat -safe 0 -i filelist.txt -c copy output.mp4
        :return:
        """
        filenames = [os.path.split(row)[-1] for row in sorted(ts_file_paths)]
        txt_content = '\n'.join([f'file {row}' for row in filenames if row.endswith('.ts')])

        txt_filename = filenames[0].replace('.ts', '.txt')
        txt_filepath = Path(self.tmp_folder) / txt_filename
        with txt_filepath.open('w+') as fp:
            fp.write(txt_content)

        dst_file = Path(self.dst) / self.filename

        # 拼接ts文件
        command = f'ffmpeg -f concat -safe 0 -i {self.tmp_folder}/{txt_filename} -c copy {dst_file}'
        print(command)
        os.system(command)

        # 删除txt文件
        if txt_filepath.exists():
            os.remove(txt_filepath)

        return dst_file

    @staticmethod
    def remove_ts_file(ts_file_paths):
        for row in ts_file_paths:
            try:
                os.remove(row)
            except Exception as e:
                print(e)

    def run(self, max_workers=None):
        """
        任务主函数
        :param max_workers: 线程池最大线程数
        """
        # 获取ts文件地址列表
        ts_urls = self.parse_m3u8_url()

        # 初始化进度条
        pbar = tqdm(total=len(ts_urls), initial=0, unit=' file', unit_scale=True, desc=self.filename, unit_divisor=1)

        # 获取ts文件保存目录
        save_folder = self.check_save_folder()

        # 创建线程池，将ts文件下载任务推入线程池
        pool = ThreadPoolExecutor(max_workers=max_workers)
        ret = [pool.submit(self.download, url, save_folder, pbar) for url in ts_urls]
        ts_file_paths = [task.result() for task in ret]

        # 关闭进度条
        pbar.close()

        # 合并ts文件
        dst_file = self.merge(ts_file_paths)

        # 删除ts文件
        if dst_file.exists():
            self.remove_ts_file(ts_file_paths)
        else:
            print('文件无法合成！！！')


if __name__ == '__main__':

    url = input('请输入m3u8地址')
    filename = input('请输入文件名称') + '.mp4'
    # exit()

    Downloader(url, filename=filename).run(max_workers=10)