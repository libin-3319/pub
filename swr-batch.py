# coding: utf-8
import cmd
import json
import logging
import shutil
from datetime import datetime
import os
from enum import Enum, unique

import openpyxl
import urllib3
from huaweicloudsdkcore.auth.credentials import BasicCredentials, GlobalCredentials
from huaweicloudsdkcore.http.http_config import HttpConfig
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkiam.v3 import IamClient, KeystoneListUsersRequest
from huaweicloudsdkiam.v3.region.iam_region import IamRegion
from huaweicloudsdkswr.v2 import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

log_path = './logs'
if not os.path.exists(log_path):
    os.makedirs(log_path)
file_handler = logging.FileHandler(log_path + os.sep + 'swr-batch.log')
file_handler.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s][%(name)-5s][%(levelname)-5s][%(funcName)s] %(message)s (%(filename)s:%(lineno)d)')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

VERSION = "v1.02"


@unique
class AUTH_OPTION(Enum):
    SELF = 1
    OTHERS = 2


@unique
class IMG_RETAINS_OPTION(Enum):
    BY_DAY = 1
    BY_NUM = 2


RESULT_FAIL = "fail"
RESULT_SUCCESS = "success"


class SWR_BATCH(object):
    def __init__(self, ak, sk, swr_endpoint,iam_endpoint):
        super().__init__()
        self.ak = ak
        self.sk = sk
        self.swr_endpoint = swr_endpoint
        self.iam_endpoint = iam_endpoint
        self.swr_client = self.get_swr_client()
        self.iam_client = self.get_iam_client()

    def get_swr_client(self):
        client = None
        try:
            credentials = BasicCredentials(self.ak, self.sk)
            client = SwrClient.new_builder() \
                .with_credentials(credentials) \
                .with_http_config(HttpConfig(ignore_ssl_verification=True)) \
                .with_endpoint(self.swr_endpoint) \
                .build()
        except Exception as e:
            logger.error("获取swr客户端异常，ak :{}".format(ak))
            logger.error(e)
        return client

    def get_iam_client(self):
        client = None
        try:
            credentials = GlobalCredentials(self.ak, self.sk)
            client = IamClient.new_builder() \
                .with_credentials(credentials) \
                .with_http_config(HttpConfig(ignore_ssl_verification=True)) \
                .with_endpoint(self.iam_endpoint) \
                .build()
        except Exception as e:
            logger.error("获取iam客户端异常，ak :{}".format(ak))
            logger.error(e)
        return client

    def get_iam_users(self):
        try:
            request = KeystoneListUsersRequest()
            response = self.iam_client.keystone_list_users(request)
            logger.info(response)
        except exceptions.ClientRequestException as e:
            logger.error("get_iam_users error")
            logger.error(e.status_code)
            logger.error(e.request_id)
            logger.error(e.error_code)
            logger.error(e.error_msg)

    def get_repository(self, namespace):
        ret = []
        try:
            request = ListReposDetailsRequest(namespace=namespace)
            response = self.swr_client.list_repos_details(request)
            if response and response.body:
                ret = response.body
        except exceptions.ClientRequestException as e:
            logger.error("get_repos error,namespace:{}".format(namespace))
            logger.error(e.status_code)
            logger.error(e.request_id)
            logger.error(e.error_code)
            logger.error(e.error_msg)
        return ret

    def read_excel(self, excel_path):
        ret = []
        work_book = openpyxl.load_workbook(excel_path)
        sheet_names = work_book.sheetnames
        sheet = work_book[sheet_names[0]]

        head_row = 1
        for row in sheet.iter_rows(min_row=head_row + 1, values_only=True):
            ret.append(row)
        return ret

    def get_repository_tag(self, namespace, repository):
        tag_list = []
        try:
            request = ListRepositoryTagsRequest()
            request.namespace = namespace
            request.repository = repository
            response = self.swr_client.list_repository_tags(request)
            if response and response.body:
                tag_list = response.body
        except exceptions.ClientRequestException as e:
            logger.error("get_repo_tags error,namespace:{},repository:{}".format(namespace, repository))
            logger.error(e.status_code)
            logger.error(e.request_id)
            logger.error(e.error_code)
            logger.error(e.error_msg)
        return tag_list

    def get_namespace_user_auth_list(self, namespace):
        auth_users = self.get_namespace_auth(namespace, AUTH_OPTION.OTHERS)
        auth_users_auth_list = []
        for auth_user in auth_users:
            auth_users_auth_list.append(auth_user)
        return auth_users_auth_list

    def get_repository_user_auth_list(self, namespace, repository):
        ret = []
        try:
            request = ShowUserRepositoryAuthRequest()
            request.namespace = namespace
            request.repository = repository
            response = self.swr_client.show_user_repository_auth(request)
            if response:
                ret = response.others_auths
        except exceptions.ClientRequestException as e:
            logger.error("get_user_repository_auth error,namespace:{},repository:{}".format(namespace, repository))
            logger.error(e.status_code)
            logger.error(e.request_id)
            logger.error(e.error_code)
            logger.error(e.error_msg)
        return ret

    def set_image_user_auth(self, excel_path):
        now_time_str = COMMON().now_time_str()
        namespace_list = [ns.name.strip() for ns in self.get_namespace()]
        excel_rows = self.read_excel(excel_path)
        user_list_cache = {}
        row_count = 1
        for row in excel_rows:
            row_count += 1
            self.log_result_to_excel(excel_path, row_count, now_time_str, RESULT_FAIL)
            try:
                namespace = self.convert_trim_str(row[0])
                # 判断namespace是否存在
                if namespace not in namespace_list:
                    logger.error("organization {} not found.".format(namespace))
                    continue

                repository = self.convert_trim_str(row[1])
                user = self.convert_trim_str(row[2])
                auth = row[3]
                if isinstance(auth, str):
                    auth = int(auth.strip())

                # 判断repository 是否存在
                repository_list = [repo.name for repo in self.get_repository(namespace)]
                if repository not in repository_list:
                    logger.error("organization {} -> repository {} not found.".format(namespace, repository))
                    continue

                # 获取此namespace下的user_auth
                target_auth_list = []
                if namespace in user_list_cache:
                    user_auth_list = user_list_cache[namespace]
                else:
                    user_auth_list = self.get_repository_user_auth_list(namespace, repository)
                    user_list_cache[namespace] = user_auth_list

                # 判断用户是否存在
                user_auth_name_list = [u.user_name.strip() for u in user_auth_list]
                if user not in user_auth_name_list:
                    logger.error(
                        "organization {} ,repository {}  -> user {} not found or can not modify for the user.".format(
                            namespace, repository, user))
                    continue

                # excel中该行的 namespace repo user 都存在的前提下修改权限，人数不多不会出现性能瓶颈
                for ua in user_auth_list:
                    if user == ua.user_name.strip():
                        ua.auth = auth
                        target_auth_list.append(ua)
                        break
                if target_auth_list:
                    ret = self.modify_repository_user_auth(namespace, repository, target_auth_list)
                    if ret and ret > 0:
                        self.log_result_to_excel(excel_path, row_count, now_time_str, RESULT_SUCCESS)

                else:
                    logger.error("image_user_auth error ")
            except Exception as e:
                logger.error(e)

    # 获取所有系统用户，待补充
    def get_all_user(self, namespace, repository):
        ret = []
        ret.append({"name": ",", "id": ""})
        return ret

    def set_org_user_auth(self, excel_path):
        now_time_str = COMMON().now_time_str()
        namespace_list = [ns.name.strip() for ns in self.get_namespace()]
        excel_rows = self.read_excel(excel_path)
        user_list_cache = {}
        row_count = 1
        for row in excel_rows:
            row_count += 1
            self.log_result_to_excel(excel_path, row_count, now_time_str, RESULT_FAIL)
            try:
                namespace = self.convert_trim_str(row[0])
                # 判断namespace是否存在
                if namespace not in namespace_list:
                    logger.error("organization {} not found.".format(namespace))
                    continue
                # 获取此namespace下的user_auth
                target_auth_list = []
                if namespace in user_list_cache:
                    user_auth_list = user_list_cache[namespace]
                else:
                    user_auth_list = self.get_namespace_user_auth_list(namespace)
                    user_list_cache[namespace] = user_auth_list

                user = self.convert_trim_str(row[1])
                auth = row[2]
                if isinstance(auth, str):
                    auth = int(auth.strip())
                # 判断用户是否存在
                user_auth_name_list = [u.user_name.strip() for u in user_auth_list]
                if user not in user_auth_name_list:
                    logger.error(
                        "organization {} -> user {} not found or can not modify for the user.".format(namespace,
                                                                                                      user))
                    continue
                # 修改权限，人数不多不会出现性能瓶颈
                for ua in user_auth_list:
                    if user == ua.user_name.strip():
                        ua.auth = auth
                        target_auth_list.append(ua)
                        break
                if target_auth_list:
                    ret = self.modify_namespace_user_auth(namespace, target_auth_list)
                    if ret and ret > 0:
                        self.log_result_to_excel(excel_path, row_count, now_time_str, RESULT_SUCCESS)
                else:
                    logger.error("org_user_auth error ")
            except Exception as e:
                logger.error(e)

    def convert_trim_str(self, value):
        ret = value
        if not isinstance(value, str):
            try:
                ret = str(value)
            except Exception as e:
                logger.info("excel cell  convert to string  error ,cell value is {}".format(value))
        return ret.strip()

    def ageing_img(self, excel_path):
        now_time_str = COMMON().now_time_str()
        namespace_list = [ns.name.strip() for ns in self.get_namespace()]
        excel_rows = self.read_excel(excel_path)
        row_count = 1
        for row in excel_rows:
            row_count += 1
            self.log_result_to_excel(excel_path, row_count, now_time_str, RESULT_FAIL)
            try:
                namespace = self.convert_trim_str(row[0])
                # 判断namespace是否存在
                if namespace not in namespace_list:
                    logger.error("organization {} not found.".format(namespace))
                    continue
                repository = self.convert_trim_str(row[1])
                retetion_type = self.convert_trim_str(row[2])
                retetion_num = self.convert_trim_str(row[3])
                # 判断repository 是否存在
                repository_list = [repo.name for repo in self.get_repository(namespace)]
                if repository not in repository_list:
                    logger.error("organization {} -> repository {} not found.".format(namespace, repository))
                    continue
                # 判断retetion_type
                result = -1
                if retetion_type.lower() == "day":
                    result = self.make_retention(namespace, repository, IMG_RETAINS_OPTION.BY_DAY, retetion_num)
                elif retetion_type.lower() == "num":
                    result = self.make_retention(namespace, repository, IMG_RETAINS_OPTION.BY_NUM, retetion_num)
                else:
                    logger.error("retetion_type from excel error, {}".format(retetion_type))

                if result and result > 0:
                    self.log_result_to_excel(excel_path, row_count, now_time_str, RESULT_SUCCESS)
            except Exception as e:
                logger.error("make_retention error,namespace:{},repository:{}".format(namespace, repository))
                logger.error(e)

    def delete_tag(self, excel_path):
        now_time_str = COMMON().now_time_str()
        namespace_list = [ns.name.strip() for ns in self.get_namespace()]
        excel_rows = self.read_excel(excel_path)
        row_count = 1
        for row in excel_rows:
            row_count += 1
            self.log_result_to_excel(excel_path, row_count, now_time_str, RESULT_FAIL)
            try:
                namespace = self.convert_trim_str(row[0])
                # 判断namespace是否存在
                if namespace not in namespace_list:
                    logger.error("organization {} not found.".format(namespace))
                    continue
                namespace = self.convert_trim_str(row[0])
                repository = self.convert_trim_str(row[1])
                tag = self.convert_trim_str(row[2])
                is_delete = self.convert_trim_str(row[3])
                # 判断repository 是否存在
                repository_list = [repo.name for repo in self.get_repository(namespace)]
                if repository not in repository_list:
                    logger.error("organization {} -> repository {} not found.".format(namespace, repository))
                    continue
                # 判断是否存在tag
                tag_list = self.get_repository_tag(namespace, repository)
                all_tag = [tag.tag for tag in tag_list]
                if tag not in all_tag:
                    logger.error(
                        "organization {} , repository {} -> tag {} not found.".format(namespace, repository, tag))
                    continue
                if is_delete.lower() == "y":
                    ret = self.delete_image_tag(namespace, repository, tag)
                    if ret and ret > 0:
                        self.log_result_to_excel(excel_path, row_count, now_time_str, RESULT_SUCCESS)
            except Exception as e:
                logger.error(e)

    def get_namespace(self):
        ret = []
        try:
            request = ListNamespacesRequest()
            response = self.swr_client.list_namespaces(request)
            if response and response.namespaces:
                ret = response.namespaces
        except exceptions.ClientRequestException as e:
            logger.error("query namespaces error: %s" % e)
            logger.error(e.status_code)
            logger.error(e.request_id)
            logger.error(e.error_code)
            logger.error(e.error_msg)
        return ret

    def get_namespace_auth(self, namespace, auth_option):
        ret = []
        try:
            request = ShowNamespaceAuthRequest()
            request.namespace = namespace
            response = self.swr_client.show_namespace_auth(request)
            if auth_option == AUTH_OPTION.OTHERS and response and response.others_auths:
                ret = response.others_auths
            elif auth_option == AUTH_OPTION.SELF and response and response.self_auth:
                ret = response.self_auth
        except exceptions.ClientRequestException as e:
            logger.error(e.status_code)
            logger.error(e.request_id)
            logger.error(e.error_code)
            logger.error(e.error_msg)
        return ret

    def modify_namespace_user_auth(self, namespace, user_auth):
        ret = -1
        try:
            request = UpdateNamespaceAuthRequest()
            request.namespace = namespace
            request.body = user_auth
            self.swr_client.update_namespace_auth(request)
            ret = 1
            logger.info(
                "org: " + namespace + " user: " + user_auth[0].user_name + " auth: " + str(
                    user_auth[0].auth) + " OK!")
        except exceptions.ClientRequestException as e:
            logger.error("modify_namespace_user_auth err!")
            logger.error(
                "org: " + namespace + " user: " + user_auth[0].user_name + " auth: " + str(
                    user_auth[0].auth) + " ERROR!")
            logger.error(e.status_code)
            logger.error(e.request_id)
            logger.error(e.error_code)
            logger.error(e.error_msg)
        return ret

    def modify_repository_user_auth(self, namespace, repository, user_auth):
        ret = -1
        try:
            request = UpdateUserRepositoryAuthRequest()
            request.namespace = namespace
            request.repository = repository
            request.body = user_auth
            self.swr_client.update_user_repository_auth(request)
            ret = 1
            logger.info(
                "org: " + namespace + " repository: " + repository + " user: " + user_auth[
                    0].user_name + " auth: " + str(user_auth[0].auth) + " OK!")
        except exceptions.ClientRequestException as e:
            logger.error("org: " + namespace + " repository: " + repository + " user: " + user_auth[
                0].user_name + " auth: " + str(user_auth[0].auth) + " ERROR!")
            logger.error(e.status_code)
            logger.error(e.request_id)
            logger.error(e.error_code)
            logger.error(e.error_msg)
        return ret

    def get_all_user_list(self, domain_id):
        pass

    def log_result_to_excel(self, excel_path, current_row_count, now_time_str, result_flag):
        try:
            # copy一份excel,添加一列result
            excel_name = os.path.basename(excel_path)
            excel_dir = os.path.dirname(excel_path)
            log_excel_dir = excel_dir + os.sep + "logs"

            if not os.path.exists(log_excel_dir):
                os.makedirs(log_excel_dir)
            log_excel_file = log_excel_dir + os.sep + "log." + now_time_str + "." + excel_name
            logger.info("log_excel_file: {}".format(log_excel_file))
            if not os.path.exists(log_excel_file):
                shutil.copy(excel_path, log_excel_file)
                workbook = openpyxl.load_workbook(log_excel_file)
                sheet = workbook.active
                # 写入表头
                sheet.cell(row=1, column=sheet.max_column + 1, value="result")
                workbook.save(log_excel_file)
                workbook.close()

            # 如果新创建的日志文件，给日志excel添加一列结果列,都置为失败
            workbook = openpyxl.load_workbook(log_excel_file)
            sheet = workbook.active
            sheet.cell(row=current_row_count, column=sheet.max_column, value=result_flag)
            workbook.save(log_excel_file)
            workbook.close()
        except Exception as e:
            logger.error("log result to excel error ,excel_path: {}".format(excel_path))
            logger.error(e)

    def delete_image_tag(self, namespace, repository, tag):
        ret = -1
        try:
            request = DeleteRepoTagRequest()
            request.namespace = namespace
            request.repository = repository
            request.tag = tag
            self.swr_client.delete_repo_tag(request)
            ret = 1
            logger.info(
                "namespace {} repository {} tag {}  delete_image_tag  OK!".format(namespace, repository, tag))
        except Exception as e:
            logger.error(
                "namespace {} repository {} tag {}  delete_image_tag  ERROR!".format(namespace, repository, tag))
            logger.error(e.status_code)
            logger.error(e.request_id)
            logger.error(e.error_code)
            logger.error(e.error_msg)
        return ret

    def get_repository_retention(self, namespace, repository):
        ret = []
        try:
            request = ListRetentionsRequest()
            request.namespace = "libin"
            request.repository = "small9"
            response = self.swr_client.list_retentions(request)
            if response and response.body:
                ret = response.body
            print(response)  # todo
        except exceptions.ClientRequestException as e:
            logger.error("get_repository_retention error,namespace {},repository {} ".format(namespace, repository))
            logger.error(e.status_code)
            logger.error(e.request_id)
            logger.error(e.error_code)
            logger.error(e.error_msg)
        return ret

    def make_retention(self, namespace, repository, img_retention_option, value):
        ret = -1
        try:
            request = CreateRetentionRequest()
            request.namespace = namespace
            request.repository = repository

            listTagSelectorsRules = []
            if img_retention_option == IMG_RETAINS_OPTION.BY_DAY:
                listRulesbody = [
                    Rule(
                        # 回收类型,date_rule:几天回收{ "days": "30" }、tag_rule:保留数目{ "num": "30" }
                        template="date_rule",
                        params={"days": value},
                        tag_selectors=listTagSelectorsRules
                    )
                ]

            elif img_retention_option == IMG_RETAINS_OPTION.BY_NUM:
                listRulesbody = [
                    Rule(
                        # 回收类型,date_rule:几天回收{ "days": "30" }、tag_rule:保留数目{ "num": "30" }
                        template="tag_rule",
                        params={"num": value},
                        tag_selectors=listTagSelectorsRules
                    )
                ]
            else:
                logging.error("img_retention_option error!")

            request.body = CreateRetentionRequestBody(
                rules=listRulesbody,
                algorithm="or"
            )
            self.swr_client.create_retention(request)
            ret = 1
        except exceptions.ClientRequestException as e:
            logging.error("-- create retention error")
            logging.error(e.status_code)
            logging.error(e.request_id)
            logging.error(e.error_code)
            logging.error(e.error_msg)
        return ret

# 公共工具
class COMMON():
    def read_json(self, key):
        with open('auth-config.json', 'r', encoding='utf-8') as f:
            json_data = json.loads(f.read())
            return json_data[key]

    def now_time_str(self):
        return datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    def utc_to_local_time_str(self, utc_time_str):
        current_timezone = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
        try:
            utc_time = datetime.datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)
            current_time = utc_time.astimezone(current_timezone)
            return current_time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error("utc time convert local time error !,utc_time_str :{} ,current_timezone: {} ".format(utc_time_str,current_timezone))
            logger.error(e)
            return "-"


class SWR_BATCH_CLI(cmd.Cmd):
    prompt = 'swr-batch-cli> '

    def __init__(self, auth_config_path):
        super().__init__()
        self.current_env = None
        self.current_auth_conf = None
        self.auth_config_path = auth_config_path
        self.envs = COMMON().load_config(self.auth_config_path)
        self.env_cache = {}
        self.swr_batch = None

    def do_all_envs(self, line):
        """show environments . usage: all_envs ."""
        for env in self.envs:
            print(env)

    def do_current_env(self, line):
        """show  current environment  . usage: current_env ."""
        if self.current_env:
            print(self.current_env)
        else:
            print("env no selected")

    def do_select_env(self, line):
        """select   current environment  . usage: select_env env ."""
        # 使用cache机制，防止过多创建客户端
        args = line.split()
        if len(args) != 1:
            print("usage: select_env env")
        else:
            if args[0] in self.envs.keys():
                self.current_env = args[0]
                self.current_auth_conf = self.envs[args[0]]
                if not self.current_env in self.env_cache:
                    self.swr_batch = SWR_BATCH(self.current_auth_conf["HUAWEICLOUD_SDK_AK"],
                                               self.current_auth_conf["HUAWEICLOUD_SDK_SK"], self.current_auth_conf["ENDPOINT"])
                    self.env_cache[self.current_env] = self.swr_batch
                else:
                    self.swr_batch = self.env_cache[self.current_env]
                logger.info("selected env  {} !".format(self.current_env))
            else:
                logger.info("env you selected :{},not exist !".format(line))

    def do_set_org_user(self, line):
        """batch set organization user auth by  excel . usage: set_org_user [excel-path]."""
        args = line.split()
        if len(args) != 1:
            print("usage: set_org_user [excel-path]")
        else:
            self.swr_batch.set_org_user_auth(args[0])

    def do_set_image_user(self, line):
        """batch set image user auth by  excel . usage: set_image_user [excel-path]."""
        args = line.split()
        if len(args) != 1:
            print("usage: set_image_user [excel-path]")
        else:
            self.swr_batch.set_image_user_auth(args[0])

    def do_delete_tag(self, line):
        """batch delete tag by   excel . usage:  delete_tag [excel-path]."""
        args = line.split()
        if len(args) != 1:
            print("usage: delete_tag [excel-path]")
        else:
            self.swr_batch.delete_tag(args[0])

    #
    # def do_aging_img(self, line):
    #     """batch aging img by   excel . usage:  aging_img [excel-path]."""
    #     args = line.split()
    #     if len(args) != 1:
    #         print("usage: aging_img [excel-path]")
    #     else:
    #         self.swr_batch.ageing_img(args[0])

    def do_quit(self, line):
        """Handle EOF (Ctrl-D or Ctrl-Z or quit) to exit."""
        return True

    def parseline(self, line):
        cmd, arg, line = super().parseline(line)
        if cmd:
            cmd = cmd.lower()
        return cmd, arg, line

    def onecmd(self, line):
        splits = line.split(None, 1)
        if not splits:
            return self.emptyline()
        command = splits[0].replace("-", "_")
        args = splits[1] if len(splits) > 1 else ''
        return super().onecmd(f"{command} {args}")

    def get_export_path(self):
        export_path = os.getcwd() + os.sep + "exports"
        if not os.path.exists(export_path):
            os.makedirs(export_path)
        return export_path


if __name__ == "__main__":
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    auth_config_path = "auth-config.json"
    common = COMMON()
    ak = common.read_json("HUAWEICLOUD_SDK_AK")
    sk = common.read_json("HUAWEICLOUD_SDK_SK")
    swr_endpoint = common.read_json("SWR_ENDPOINT")
    iam_endpoint = common.read_json("IAM_ENDPOINT")

    # tip = "Welcome to swr-batch-cli(%s). Type 'help' or '?' to list commands. Type 'quit' to exit." % VERSION
    # app = SWR_BATCH_CLI(auth_config_path)
    # app.do_select_env("cn-east-2")
    # app.do_select_env("cn-east-2")
    # app.cmdloop(tip)

    batch = SWR_BATCH(ak, sk, swr_endpoint,iam_endpoint)
    users = batch.get_iam_users()
    pass
