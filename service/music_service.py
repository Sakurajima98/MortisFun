#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目音乐服务模块

本文件实现音乐消息发送功能，包括：
1. 网络音乐搜索
2. 音乐链接获取
3. 音乐消息发送
4. 音乐服务管理

作者: Mortisfun Team
创建时间: 2025
"""

import logging
from typing import Dict, Any, Optional, List
from .base_service import BaseService

try:
    import pyncm
    from pyncm import GetCurrentSession, SetCurrentSession, DumpSessionAsString, LoadSessionFromString
    from pyncm.apis.cloudsearch import GetSearchResult
    from pyncm.apis.track import GetTrackAudio
    from pyncm.apis.login import LoginViaCellphone, LoginViaEmail, LoginViaCookie
    from pyncm.apis.login import SetSendRegisterVerifcationCodeViaCellphone
    # SessionManager 在新版本pyncm中不存在，移除此导入
except ImportError:
    pyncm = None
    logging.warning("pyncm库未安装，音乐服务将无法正常工作")


class MusicService(BaseService):
    """
    音乐服务类
    
    负责处理音乐消息的搜索和发送，包括网络音乐搜索、
    链接获取和消息构建等功能。
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, napcat_api_caller, server=None):
        """
        初始化音乐服务
        
        Args:
            config (Dict[str, Any]): 配置信息
            data_manager: 数据管理器
            text_formatter: 文本格式化器
            napcat_api_caller: Napcat API调用器
            server: 服务器实例，用于日志格式化
        """
        super().__init__(config, data_manager, text_formatter, server)
        self.napcat_api_caller = napcat_api_caller
        
        # 添加权限控制常量
        self.ALLOWED_RELOGIN_USERS = ['2627852499']  # 允许使用重新登录功能的QQ号列表
        
        # 音乐服务配置 - 修复配置路径读取
        music_config = config.get('music', {})
        self.search_limit = music_config.get('search_limit', 5)
        self.bitrate = music_config.get('bitrate', 320000)
        
        # 网易云音乐登录配置
        self.netease_config = music_config.get('netease_cloud', {})
        self.login_enabled = self.netease_config.get('login_enabled', False)
        self.login_info_file = self.netease_config.get('login_info_file', 'data/netease_login.json')
        # 新增：会话持久化文件路径，用于跨进程/重启后维持登录态
        self.session_file = self.netease_config.get('session_file', 'data/netease_session.dat')
        
        # 临时存储两步验证登录的手机号
        self.temp_login_phone = None
        
        # 检查pyncm是否可用
        if pyncm is None:
            self.log_unified("ERROR", "pyncm库未安装，音乐服务无法正常工作", group_id="system", user_id="system")
            self.enabled = False
        else:
            self.enabled = True
            # 初始化网易云音乐会话
            try:
                self.session = GetCurrentSession()
                # 如果启用登录，尝试登录
                if self.login_enabled:
                    self._login_netease_cloud()
                self.log_unified("INFO", "网易云音乐会话初始化成功", group_id="system", user_id="system")
            except Exception as e:
                self.log_unified("ERROR", f"网易云音乐会话初始化失败: {e}", group_id="system", user_id="system")
                self.enabled = False
    
    def _login_netease_cloud(self):
        """
        登录网易云音乐账户
        
        支持手机号、邮箱和Cookie三种登录方式
        根据pyncm库的正确API实现登录功能
        """
        try:
            # 尝试从文件加载已保存的登录信息
            if self._load_saved_login():
                self.log_unified("INFO", "使用已保存的登录信息登录成功", group_id="system", user_id="system")
                return
            
            login_method = self.netease_config.get('login_method', 'phone')
            
            if login_method == 'phone':
                phone = self.netease_config.get('phone', '')
                password = self.netease_config.get('password', '')
                captcha = self.netease_config.get('captcha', '')  # 验证码
                use_captcha = self.netease_config.get('use_captcha', False)  # 是否使用验证码登录
                
                if phone:
                    try:
                        # 根据pyncm文档，如果需要验证码登录
                        if use_captcha and captcha:
                            self.log_unified("INFO", f"使用验证码登录: {phone}", group_id="system", user_id="system")
                            result = LoginViaCellphone(
                                phone=phone,
                                captcha=captcha,
                                ctcode=86,  # 中国区号
                                remeberLogin=True,
                                session=self.session
                            )
                        elif password:
                            self.log_unified("INFO", f"使用密码登录: {phone}", group_id="system", user_id="system")
                            result = LoginViaCellphone(
                                phone=phone, 
                                password=password, 
                                ctcode=86,  # 中国区号
                                remeberLogin=True,
                                session=self.session
                            )
                        else:
                            self.log_unified("WARNING", "手机号登录需要密码或验证码，跳过登录", group_id="system", user_id="system")
                            return
                        
                        # 检查登录结果
                        if result and result.get('code') == 200:
                            self.log_unified("INFO", f"手机号登录成功: {phone}", group_id="system", user_id="system")
                            self._save_login_info()
                        else:
                            error_msg = result.get('message', '未知错误') if result else '登录返回空结果'
                            self.log_unified("ERROR", f"手机号登录失败: {error_msg}", group_id="system", user_id="system")
                            # 如果登录失败，记录详细错误信息
                            if result:
                                self.log_unified("ERROR", f"登录失败详情: {result}", group_id="system", user_id="system")
                                
                            # 如果是密码登录失败且提示需要验证码，给出提示
                            if not use_captcha and result and 'captcha' in str(result).lower():
                                self.log_unified("INFO", "提示: 可能需要验证码登录，请在配置中设置use_captcha=true并提供验证码", group_id="system", user_id="system")
                                
                    except Exception as e:
                        self.log_unified("ERROR", f"手机号登录异常: {e}", group_id="system", user_id="system")
                        import traceback
                        self.log_unified("ERROR", f"登录异常详情: {traceback.format_exc()}", group_id="system", user_id="system")
                else:
                    self.log_unified("WARNING", "手机号未配置，跳过登录", group_id="system", user_id="system")
                    
            elif login_method == 'email':
                email = self.netease_config.get('email', '')
                password = self.netease_config.get('password', '')
                
                if email and password:
                    result = LoginViaEmail(email=email, password=password)
                    if result and result.get('code') == 200:
                        self.log_unified("INFO", f"邮箱登录成功: {email}", group_id="system", user_id="system")
                        self._save_login_info()
                    else:
                        error_msg = result.get('message', '未知错误') if result else '登录返回空结果'
                        self.log_unified("ERROR", f"邮箱登录失败: {error_msg}", group_id="system", user_id="system")
                else:
                    self.log_unified("WARNING", "邮箱或密码未配置，跳过登录", group_id="system", user_id="system")
                    
            elif login_method == 'cookie':
                cookie_music_u = self.netease_config.get('cookie_music_u', '')
                
                if cookie_music_u:
                    result = LoginViaCookie(MUSIC_U=cookie_music_u)
                    if result and result.get('code') == 200:
                        self.log_unified("INFO", "Cookie登录成功", group_id="system", user_id="system")
                        self._save_login_info()
                    else:
                        error_msg = result.get('message', '未知错误') if result else '登录返回空结果'
                        self.log_unified("ERROR", f"Cookie登录失败: {error_msg}", group_id="system", user_id="system")
                else:
                    self.log_unified("WARNING", "MUSIC_U Cookie未配置，跳过登录", group_id="system", user_id="system")
            
            # === 新增：顺序回退登录模式 ===
            elif login_method in ('all', '全部'):
                # 说明：按优先级依次尝试以下方式，任一成功即停止：
                # 1) Cookie 登录（最快，依赖有效的MUSIC_U）
                # 2) 手机号+密码 登录
                # 3) 手机号+验证码 登录（需use_captcha=true且提供captcha）
                # 4) 邮箱+密码 登录
                error_msgs = []  # 收集失败原因以便最终提示

                # 读取通用配置
                cookie_music_u = self.netease_config.get('cookie_music_u', '')
                phone = self.netease_config.get('phone', '')
                password = self.netease_config.get('password', '')
                use_captcha = self.netease_config.get('use_captcha', False)
                captcha = self.netease_config.get('captcha', '')
                email = self.netease_config.get('email', '')

                # 1) Cookie 登录
                try:
                    if cookie_music_u:
                        self.log_unified("INFO", "[ALL] 尝试使用Cookie登录 ...", group_id="system", user_id="system")
                        result = LoginViaCookie(MUSIC_U=cookie_music_u)
                        if result and result.get('code') == 200:
                            self.log_unified("INFO", "[ALL] Cookie登录成功", group_id="system", user_id="system")
                            self._save_login_info()
                            return
                        else:
                            msg = result.get('message', '未知错误') if result else '登录返回空结果'
                            self.log_unified("WARNING", f"[ALL] Cookie登录失败: {msg}", group_id="system", user_id="system")
                            error_msgs.append(f"Cookie: {msg}")
                    else:
                        self.log_unified("INFO", "[ALL] 跳过Cookie：未配置MUSIC_U", group_id="system", user_id="system")
                except Exception as e:
                    self.log_unified("ERROR", f"[ALL] Cookie登录异常: {e}", group_id="system", user_id="system")
                    error_msgs.append(f"Cookie异常: {e}")

                # 2) 手机号+密码 登录
                try:
                    if phone and password:
                        self.log_unified("INFO", f"[ALL] 尝试手机号密码登录: {phone}", group_id="system", user_id="system")
                        result = LoginViaCellphone(
                            phone=phone,
                            password=password,
                            ctcode=86,
                            remeberLogin=True,
                            session=self.session
                        )
                        if result and result.get('code') == 200:
                            self.log_unified("INFO", "[ALL] 手机号密码登录成功", group_id="system", user_id="system")
                            self._save_login_info()
                            return
                        else:
                            msg = result.get('message', '未知错误') if result else '登录返回空结果'
                            self.log_unified("WARNING", f"[ALL] 手机号密码登录失败: {msg}", group_id="system", user_id="system")
                            error_msgs.append(f"Phone+Password: {msg}")
                    else:
                        self.log_unified("INFO", "[ALL] 跳过手机号密码：未配置phone/password", group_id="system", user_id="system")
                except Exception as e:
                    self.log_unified("ERROR", f"[ALL] 手机号密码登录异常: {e}", group_id="system", user_id="system")
                    error_msgs.append(f"Phone+Password异常: {e}")

                # 3) 手机号+验证码 登录（仅当配置要求使用验证码且提供验证码时尝试）
                try:
                    if phone and use_captcha and captcha:
                        self.log_unified("INFO", f"[ALL] 尝试手机号验证码登录: {phone}", group_id="system", user_id="system")
                        result = LoginViaCellphone(
                            phone=phone,
                            captcha=captcha,
                            ctcode=86,
                            remeberLogin=True,
                            session=self.session
                        )
                        if result and result.get('code') == 200:
                            self.log_unified("INFO", "[ALL] 手机号验证码登录成功", group_id="system", user_id="system")
                            self._save_login_info()
                            return
                        else:
                            msg = result.get('message', '验证码错误或已过期') if result else '登录返回空结果'
                            self.log_unified("WARNING", f"[ALL] 手机号验证码登录失败: {msg}", group_id="system", user_id="system")
                            error_msgs.append(f"Phone+Captcha: {msg}")
                    else:
                        self.log_unified("INFO", "[ALL] 跳过手机号验证码：需use_captcha=true且提供captcha", group_id="system", user_id="system")
                except Exception as e:
                    self.log_unified("ERROR", f"[ALL] 手机号验证码登录异常: {e}", group_id="system", user_id="system")
                    error_msgs.append(f"Phone+Captcha异常: {e}")

                # 4) 邮箱+密码 登录
                try:
                    if email and password:
                        self.log_unified("INFO", f"[ALL] 尝试邮箱登录: {email}", group_id="system", user_id="system")
                        result = LoginViaEmail(email=email, password=password)
                        if result and result.get('code') == 200:
                            self.log_unified("INFO", "[ALL] 邮箱登录成功", group_id="system", user_id="system")
                            self._save_login_info()
                            return
                        else:
                            msg = result.get('message', '未知错误') if result else '登录返回空结果'
                            self.log_unified("WARNING", f"[ALL] 邮箱登录失败: {msg}", group_id="system", user_id="system")
                            error_msgs.append(f"Email: {msg}")
                    else:
                        self.log_unified("INFO", "[ALL] 跳过邮箱：未配置email/password", group_id="system", user_id="system")
                except Exception as e:
                    self.log_unified("ERROR", f"[ALL] 邮箱登录异常: {e}", group_id="system", user_id="system")
                    error_msgs.append(f"Email异常: {e}")

                # 全部失败：记录汇总信息
                if error_msgs:
                    self.log_unified("WARNING", f"[ALL] 全部登录方式均尝试失败 -> {' | '.join(error_msgs)}", group_id="system", user_id="system")
                else:
                    self.log_unified("WARNING", "[ALL] 无可用的登录方式（配置缺失），已跳过登录", group_id="system", user_id="system")
                    
        except Exception as e:
            self.log_unified("ERROR", f"网易云音乐登录失败: {e}", group_id="system", user_id="system")
            # 记录详细的异常信息用于调试
            import traceback
            self.log_unified("ERROR", f"登录异常详情: {traceback.format_exc()}", group_id="system", user_id="system")
    
    def send_verification_code(self, phone: str, ctcode: int = 86) -> Dict[str, Any]:
        """
        发送手机验证码（用于登录）
        
        Args:
            phone (str): 手机号
            ctcode (int): 国家代码，默认86（中国）
            
        Returns:
            Dict[str, Any]: 发送结果
        """
        try:
            self.log_unified("INFO", f"正在为手机号 {phone} 发送登录验证码", group_id="system", user_id="system")
            
            # 网易云音乐的验证码登录机制：
            # 1. 使用注册验证码API发送验证码
            # 2. 然后使用LoginViaCellphone的captcha参数登录
            result = SetSendRegisterVerifcationCodeViaCellphone(
                cell=phone,
                ctcode=ctcode
            )
            
            self.log_unified("INFO", f"验证码发送API返回: {result}", group_id="system", user_id="system")
            
            if result and result.get('code') == 200:
                self.log_unified("INFO", f"验证码发送成功: {phone}", group_id="system", user_id="system")
                return {
                    'success': True,
                    'message': '验证码发送成功，请查收短信（5分钟内有效）',
                    'phone': phone,
                    'result': result
                }
            else:
                error_msg = result.get('message', '未知错误') if result else '发送返回空结果'
                self.log_unified("ERROR", f"验证码发送失败: {error_msg}, 完整结果: {result}", group_id="system", user_id="system")
                return {
                    'success': False,
                    'message': f'验证码发送失败: {error_msg}',
                    'phone': phone,
                    'result': result
                }
                
        except Exception as e:
            self.log_unified("ERROR", f"发送验证码异常: {e}", group_id="system", user_id="system")
            import traceback
            self.log_unified("ERROR", f"发送验证码异常详情: {traceback.format_exc()}", group_id="system", user_id="system")
            return {
                'success': False,
                'message': f'发送验证码异常: {str(e)}',
                'phone': phone
            }
    
    def login_with_verification_code(self, verification_code: str) -> Dict[str, Any]:
        """
        使用验证码登录网易云音乐
        
        Args:
            verification_code (str): 手机验证码
            
        Returns:
            Dict[str, Any]: 登录结果
        """
        try:
            if not self.temp_login_phone:
                return {
                    'success': False,
                    'message': '❌ 请先使用 /音乐重新登录 手机号 发送验证码'
                }
            
            self.log_unified("INFO", f"正在使用验证码登录，手机号: {self.temp_login_phone}", group_id="system", user_id="system")
            
            # 使用pyncm进行验证码登录
            # 注意：使用captcha参数进行验证码登录，需要先发送验证码
            result = LoginViaCellphone(
                phone=self.temp_login_phone,
                captcha=verification_code,
                ctcode=86,
                remeberLogin=True
            )
            
            self.log_unified("INFO", f"验证码登录API返回: {result}", group_id="system", user_id="system")
            
            if result and result.get('code') == 200:
                self.log_unified("INFO", f"验证码登录成功: {self.temp_login_phone}", group_id="system", user_id="system")
                
                # 保存登录信息
                self._save_login_info()
                
                # 获取用户信息
                profile = result.get('profile', {})
                nickname = profile.get('nickname', '未知用户')
                user_id = profile.get('userId', 0)
                
                # 清除临时手机号
                phone = self.temp_login_phone
                self.temp_login_phone = None
                
                return {
                    'success': True,
                    'message': f"✅ 登录成功！\n👤 用户: {nickname}\n🆔 用户ID: {user_id}\n📱 手机号: {phone}",
                    'user_info': {
                        'nickname': nickname,
                        'user_id': user_id
                    },
                    'result': result
                }
            else:
                error_msg = result.get('message', '验证码错误或已过期') if result else '登录返回空结果'
                self.log_unified("ERROR", f"验证码登录失败: {error_msg}, 完整结果: {result}", group_id="system", user_id="system")
                return {
                    'success': False,
                    'message': f'❌ 登录失败: {error_msg}\n💡 请检查验证码是否正确或重新获取验证码',
                    'result': result
                }
                
        except Exception as e:
            # 特殊处理pyncm的LoginFailedException
            if 'LoginFailedException' in str(type(e)):
                error_info = str(e)
                self.log_unified("ERROR", f"验证码登录失败: {error_info}", group_id="system", user_id="system")
                return {
                    'success': False,
                    'message': f'❌ 验证码登录失败: {error_info}\n💡 请检查验证码是否正确或重新获取验证码'
                }
            else:
                self.log_unified("ERROR", f"验证码登录异常: {e}", group_id="system", user_id="system")
                import traceback
                self.log_unified("ERROR", f"验证码登录异常详情: {traceback.format_exc()}", group_id="system", user_id="system")
                return {
                    'success': False,
                    'message': f'❌ 登录异常: {str(e)}'
                }
    
    def _load_saved_login(self) -> bool:
        """
        加载已保存的登录信息（优先加载会话转储以恢复登录态）
        
        Returns:
            bool: 是否成功加载并验证登录态
        """
        try:
            # 优先加载会话（包含Cookie、设备ID等），若验证通过则直接复用
            if self._load_saved_session():
                self.log_unified("INFO", "已从会话文件恢复网易云登录态", group_id="system", user_id="system")
                return True
        except Exception as e:
            self.log_unified("WARNING", f"从会话文件加载失败，将回退到旧登录信息检查: {e}", group_id="system", user_id="system")
        
        # 旧逻辑仅记录时间戳，不含有效凭证，此处仅做存在性与有效期检查，不作为跳过登录的依据
        try:
            import os
            import json
            if not os.path.exists(self.login_info_file):
                return False
            with open(self.login_info_file, 'r', encoding='utf-8') as f:
                login_data = json.load(f)
            if 'login_time' in login_data:
                from datetime import datetime, timedelta
                login_time = datetime.fromisoformat(login_data['login_time'])
                if datetime.now() - login_time < timedelta(days=30):
                    # 仅表示历史上登录过，但不代表当前会话有效；返回False以触发正常登录流程
                    self.log_unified("INFO", "检测到历史登录记录，但未找到有效会话，继续执行登录流程", group_id="system", user_id="system")
                    return False
        except Exception as e:
            self.log_unified("ERROR", f"加载登录信息失败: {e}", group_id="system", user_id="system")
        return False
    
    def _save_login_info(self):
        """
        保存登录信息到文件
        """
        try:
            if not self.netease_config.get('save_login_info', True):
                self.log_unified("INFO", "登录信息保存功能已禁用", group_id="system", user_id="system")
                return
                
            import os
            import json
            from datetime import datetime
            
            # 确保目录存在
            login_dir = os.path.dirname(self.login_info_file)
            session_dir = os.path.dirname(self.session_file)
            
            try:
                os.makedirs(login_dir, exist_ok=True)
                self.log_unified("DEBUG", f"确保登录信息目录存在: {login_dir}", group_id="system", user_id="system")
            except Exception as e:
                self.log_unified("ERROR", f"创建登录信息目录失败: {e}", group_id="system", user_id="system")
                return
                
            try:
                os.makedirs(session_dir, exist_ok=True)
                self.log_unified("DEBUG", f"确保会话目录存在: {session_dir}", group_id="system", user_id="system")
            except Exception as e:
                self.log_unified("ERROR", f"创建会话目录失败: {e}", group_id="system", user_id="system")
                return
            
            # 保存登录时间信息（元数据）
            login_info = {
                'login_time': datetime.now().isoformat(),
                'login_method': self.netease_config.get('login_method', 'phone'),
                'phone': self.netease_config.get('phone', ''),
                'success': True
            }
            
            with open(self.login_info_file, 'w', encoding='utf-8') as f:
                json.dump(login_info, f, ensure_ascii=False, indent=2)
            
            # 同步保存会话（包含Cookie等），用于跨重启保持登录状态
            self._save_session_to_file()
            
            # 获取当前时间戳
            self.log_unified("INFO", f"登录信息与会话已保存到: {self.login_info_file} / {self.session_file}", group_id="system", user_id="system")
            
        except Exception as e:
            self.log_unified("ERROR", f"保存登录信息失败: {e}", group_id="system", user_id="system")
    
    def _save_session_to_file(self) -> None:
        """
        将当前pyncm会话序列化并保存到磁盘，以便重启后恢复登录态。
        注意：文件中包含Cookies等敏感信息，请妥善保管。
        """
        try:
            if pyncm is None:
                self.log_unified("WARNING", "pyncm库不可用，无法保存会话", group_id="system", user_id="system")
                return
                
            self.log_unified("DEBUG", "开始保存网易云音乐会话到文件", group_id="system", user_id="system")
            
            # 从当前会话导出字符串
            dump_str = DumpSessionAsString(GetCurrentSession())
            if not dump_str:
                self.log_unified("WARNING", "会话导出为空，跳过保存", group_id="system", user_id="system")
                return
                
            # 写入文件
            import os
            session_dir = os.path.dirname(self.session_file)
            try:
                os.makedirs(session_dir, exist_ok=True)
                self.log_unified("DEBUG", f"确保会话目录存在: {session_dir}", group_id="system", user_id="system")
            except Exception as e:
                self.log_unified("ERROR", f"创建会话目录失败: {e}", group_id="system", user_id="system")
                return
                
            with open(self.session_file, 'w', encoding='utf-8') as f:
                f.write(dump_str)
            self.log_unified("INFO", f"会话已成功保存到: {self.session_file}", group_id="system", user_id="system")
            
        except Exception as e:
            self.log_unified("ERROR", f"保存会话到文件失败: {e}", group_id="system", user_id="system")
            import traceback
            self.log_unified("ERROR", f"保存会话异常详情: {traceback.format_exc()}", group_id="system", user_id="system")
    
    def _load_saved_session(self) -> bool:
        """
        从磁盘加载已保存的pyncm会话并设置为当前会话，然后验证登录状态。
        
        Returns:
            bool: 如成功恢复且处于已登录状态返回True，否则False
        """
        try:
            if pyncm is None:
                self.log_unified("WARNING", "pyncm库不可用，无法加载会话", group_id="system", user_id="system")
                return False
                
            import os
            if not os.path.exists(self.session_file):
                self.log_unified("DEBUG", f"会话文件不存在: {self.session_file}", group_id="system", user_id="system")
                return False
                
            self.log_unified("DEBUG", f"开始从文件加载会话: {self.session_file}", group_id="system", user_id="system")
            
            # 读取并反序列化会话
            with open(self.session_file, 'r', encoding='utf-8') as f:
                dump_str = f.read().strip()
                
            if not dump_str:
                self.log_unified("WARNING", "会话文件为空", group_id="system", user_id="system")
                return False
                
            session = LoadSessionFromString(dump_str)
            if not session:
                self.log_unified("WARNING", "会话反序列化失败", group_id="system", user_id="system")
                return False
                
            # 切换当前全局会话
            SetCurrentSession(session)
            self.session = session
            self.log_unified("DEBUG", "会话已成功加载并设置为当前会话", group_id="system", user_id="system")
            
            # 验证是否处于登录态
            status = self.get_login_status()
            is_logged_in = bool(status.get('logged_in'))
            
            if is_logged_in:
                nickname = status.get('nickname', '未知用户')
                self.log_unified("INFO", f"会话验证成功，用户: {nickname}", group_id="system", user_id="system")
            else:
                self.log_unified("WARNING", "会话加载成功但验证失败，可能已过期", group_id="system", user_id="system")
                
            return is_logged_in
            
        except Exception as e:
            self.log_unified("ERROR", f"加载会话失败: {e}", group_id="system", user_id="system")
            import traceback
            self.log_unified("ERROR", f"加载会话异常详情: {traceback.format_exc()}", group_id="system", user_id="system")
            return False
    
    def relogin_netease_cloud(self) -> Dict[str, Any]:
        """
        重新登录网易云音乐账户
        
        当账户过期或登录失效时，可以使用此方法重新登录
        
        Returns:
            Dict[str, Any]: 登录结果信息
        """
        try:
            # 清除旧的登录信息文件
            import os
            if os.path.exists(self.login_info_file):
                os.remove(self.login_info_file)
                self.log_unified("INFO", "已清除旧的登录信息", group_id="system", user_id="system")
            # 清除旧的会话转储文件
            if os.path.exists(self.session_file):
                os.remove(self.session_file)
                self.log_unified("INFO", "已清除旧的会话文件", group_id="system", user_id="system")
            
            # 重新初始化会话
            self.session = GetCurrentSession()
            
            # 如果启用登录，尝试重新登录
            if self.login_enabled:
                self._login_netease_cloud()
                
                # 检查登录状态
                status = self.get_login_status()
                if status.get('logged_in'):
                    return {
                        'success': True,
                        'message': f"✅ 重新登录成功！\n👤 用户: {status['nickname']}\n🎖️ 状态: {'会员' if status['is_vip'] else '普通用户'}",
                        'user_info': {
                            'nickname': status['nickname'],
                            'user_id': status['user_id'],
                            'is_vip': status['is_vip']
                        }
                    }
                else:
                    return {
                        'success': False,
                        'message': "❌ 重新登录失败\n💡 请检查配置文件中的登录信息是否正确"
                    }
            else:
                return {
                    'success': False,
                    'message': "❌ 登录功能未启用\n💡 请在配置文件中启用登录功能"
                }
                
        except Exception as e:
            self.log_unified("ERROR", f"重新登录失败: {e}", group_id="system", user_id="system")
            return {
                'success': False,
                'message': f"❌ 重新登录过程中发生错误: {str(e)}"
            }
    
    def login_with_password(self) -> Dict[str, Any]:
        """
        使用账号密码登录网易云音乐
        
        从config配置文件中读取账号密码进行登录，
        不依赖验证码，直接使用密码认证。
        
        Returns:
            Dict[str, Any]: 登录结果信息
        """
        try:
            # 检查是否已经登录
            current_status = self.get_login_status()
            if current_status.get('logged_in'):
                return {
                    'success': True,
                    'message': f"✅ 已经登录！\n👤 用户: {current_status['nickname']}\n🎖️ 状态: {'会员' if current_status['is_vip'] else '普通用户'}",
                    'user_info': {
                        'nickname': current_status['nickname'],
                        'user_id': current_status['user_id'],
                        'is_vip': current_status['is_vip']
                    }
                }
            
            # 从配置中获取账号密码
            phone = self.netease_config.get('phone', '')
            password = self.netease_config.get('password', '')
            
            if not phone or not password:
                return {
                    'success': False,
                    'message': "❌ 账号密码登录失败\n💡 请在config.json中配置phone和password"
                }
            
            self.log_unified("INFO", f"开始使用账号密码登录: {phone}", group_id="system", user_id="system")
            
            # 使用密码登录
            result = LoginViaCellphone(
                phone=phone,
                password=password,
                ctcode=86,  # 中国区号
                remeberLogin=True,
                session=self.session
            )
            
            # 检查登录结果
            if result and result.get('code') == 200:
                self.log_unified("INFO", f"账号密码登录成功: {phone}", group_id="system", user_id="system")
                
                # 保存登录信息
                self._save_login_info()
                
                # 获取用户信息
                status = self.get_login_status()
                
                return {
                    'success': True,
                    'message': f"✅ 账号密码登录成功！\n👤 用户: {status['nickname']}\n🎖️ 状态: {'会员' if status['is_vip'] else '普通用户'}",
                    'user_info': {
                        'nickname': status['nickname'],
                        'user_id': status['user_id'],
                        'is_vip': status['is_vip']
                    }
                }
            else:
                error_msg = result.get('message', '未知错误') if result else '登录返回空结果'
                error_code = result.get('code', 'unknown') if result else 'unknown'
                
                self.log_unified("ERROR", f"账号密码登录失败: {error_msg} (错误码: {error_code})", group_id="system", user_id="system")
                
                # 根据错误码提供更详细的提示
                if error_code == 502:
                    tip_msg = "\n💡 提示: 密码错误，请检查config.json中的密码是否正确"
                elif error_code == 400:
                    tip_msg = "\n💡 提示: 请求参数错误，请检查手机号格式"
                elif error_code == 460:
                    tip_msg = "\n💡 提示: 账号或密码错误"
                else:
                    tip_msg = "\n💡 提示: 请检查网络连接和账号密码是否正确"
                
                return {
                    'success': False,
                    'message': f"❌ 账号密码登录失败\n🔍 错误: {error_msg} (错误码: {error_code}){tip_msg}"
                }
                
        except Exception as e:
            self.log_unified("ERROR", f"账号密码登录异常: {e}", group_id="system", user_id="system")
            import traceback
            self.log_unified("ERROR", f"登录异常详情: {traceback.format_exc()}", group_id="system", user_id="system")
            
            return {
                'success': False,
                'message': f"❌ 账号密码登录过程中发生错误: {str(e)}"
            }
    
    def get_available_music_sources(self) -> List[str]:
        """
        获取可用的音乐源列表
        
        Returns:
            List[str]: 音乐源列表
        """
        sources = []
        if self.enabled:
            sources.append("网易云音乐")
        
        self.log_unified("INFO", f"找到 {len(sources)} 个可用音乐源", group_id="system", user_id="system")
        return sources
    
    def search_music(self, keyword: str, limit: int = None) -> Optional[List[Dict[str, Any]]]:
        """
        搜索音乐
        
        Args:
            keyword (str): 搜索关键词
            limit (int): 搜索结果数量限制
            
        Returns:
            Optional[List[Dict[str, Any]]]: 搜索结果列表或None
        """
        if not self.enabled:
            return None
            
        if limit is None:
            limit = self.search_limit
            
        try:
            # 使用pyncm搜索音乐
            result = GetSearchResult(keyword, stype=1, limit=limit)
            if result and 'result' in result and 'songs' in result['result']:
                songs = result['result']['songs']
                self.log_unified("INFO", f"搜索到 {len(songs)} 首歌曲: {keyword}", group_id="system", user_id="system")
                return songs
            return None
        except Exception as e:
            self.log_unified("ERROR", f"搜索音乐失败: {e}", group_id="system", user_id="system")
            return None
    
    def get_music_url(self, song_id: int) -> Optional[str]:
        """
        获取音乐播放链接
        
        Args:
            song_id (int): 歌曲ID
            
        Returns:
            Optional[str]: 音乐播放链接或None
        """
        if not self.enabled:
            return None
            
        try:
            # 获取音乐播放链接
            result = GetTrackAudio([song_id], bitrate=self.bitrate)
            if result and 'data' in result and len(result['data']) > 0:
                audio_data = result['data'][0]
                if 'url' in audio_data and audio_data['url']:
                    self.log_unified("INFO", f"获取音乐链接成功: {song_id}", group_id="system", user_id="system")
                    return audio_data['url']
            
            self.log_unified("WARNING", f"无法获取音乐链接: {song_id}", group_id="system", user_id="system")
            return None
        except Exception as e:
            self.log_unified("ERROR", f"获取音乐链接失败: {e}", group_id="system", user_id="system")
            return None
    
    def format_song_info(self, song: Dict[str, Any]) -> str:
        """
        格式化歌曲信息
        
        Args:
            song (Dict[str, Any]): 歌曲信息
            
        Returns:
            str: 格式化后的歌曲信息
        """
        song_title = song.get('name', '未知歌曲')
        artists = song.get('ar', [])
        artist_names = ', '.join([artist.get('name', '') for artist in artists])
        album = song.get('al', {}).get('name', '未知专辑')
        
        return f"🎵 {song_title}\n🎤 歌手: {artist_names}\n💿 专辑: {album}"
    
    def get_login_status(self) -> Dict[str, Any]:
        """
        获取当前登录状态
        
        Returns:
            Dict[str, Any]: 登录状态信息
        """
        # 检查pyncm是否可用
        if not self.enabled or pyncm is None:
            return {'logged_in': False, 'error': 'pyncm库不可用'}
            
        try:
            from pyncm.apis.login import GetCurrentLoginStatus
            
            status = GetCurrentLoginStatus()
            if status.get('code') == 200:
                profile = status.get('profile', {})
                return {
                    'logged_in': True,
                    'user_id': profile.get('userId'),
                    'nickname': profile.get('nickname'),
                    'vip_type': profile.get('vipType', 0),
                    'is_vip': profile.get('vipType', 0) > 0
                }
            else:
                return {'logged_in': False}
                
        except Exception as e:
            self.log_unified("ERROR", f"获取登录状态失败: {e}", group_id="system", user_id="system")
            return {'logged_in': False}
    
    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理音乐相关消息
        
        Args:
            message (str): 用户消息
            user_id (str): 用户ID
            **kwargs: 其他参数
            
        Returns:
            Optional[Dict[str, Any]]: 处理结果
        """
        try:
            message = message.strip()
            
            # 音乐源列表命令
            if message in ['/音乐源', '/music_sources']:
                sources = self.get_available_music_sources()
                if not sources:
                    return {
                        "content": "❌ 当前没有可用的音乐源",
                        "music_url": None
                    }
                
                source_list = "\n".join([f"🎵 {source}" for source in sources])
                content = f"🎵 可用音乐源列表：\n{source_list}\n\n💡 使用方法：/听歌 歌曲名"
                return {
                    "content": content,
                    "music_url": None
                }
            
            # 检查登录状态
            if message.startswith('/音乐登录状态'):
                if not self.enabled:
                    return {
                        "content": "❌ 音乐服务不可用\n" +
                                  "💡 提示: pyncm库未正确安装，请检查依赖库安装情况",
                        "music_url": None
                    }
                    
                status = self.get_login_status()
                if status.get('error'):
                    return {
                        "content": f"❌ 音乐服务错误: {status['error']}\n" +
                                  "💡 提示: 请检查pyncm库是否正确安装",
                        "music_url": None
                    }
                elif status['logged_in']:
                    vip_status = "会员" if status['is_vip'] else "普通用户"
                    return {
                        "content": f"🎵 网易云音乐登录状态:\n" +
                                  f"✅ 已登录\n" +
                                  f"👤 用户: {status['nickname']}\n" +
                                  f"🎖️ 状态: {vip_status}\n" +
                                  f"🆔 用户ID: {status['user_id']}",
                        "music_url": None
                    }
                else:
                    return {
                        "content": "❌ 网易云音乐未登录\n" +
                                  "💡 提示: 请在配置文件中设置登录信息并重启服务，或使用 /音乐重新登录 指令",
                        "music_url": None
                    }
            
            # 重新登录指令（智能登录）- 需要权限验证
            if message.startswith('/音乐重新登录') or message.startswith('/music_relogin'):
                # 权限检查：只允许指定用户使用重新登录功能
                if user_id not in self.ALLOWED_RELOGIN_USERS:
                    return {
                        "content": "❌ 权限不足\n💡 提示: 该功能仅限管理员使用",
                        "music_url": None
                    }
                
                if not self.enabled:
                    return {
                        "content": "❌ 音乐服务不可用\n" +
                                  "💡 提示: pyncm库未正确安装，请检查依赖库安装情况",
                        "music_url": None
                    }
                
                # 解析手机号
                parts = message.split()
                if len(parts) < 2:
                    return {
                        "content": "❌ 请提供手机号\n💡 使用方法: /音乐重新登录 手机号",
                        "music_url": None
                    }
                
                phone = parts[1].strip()
                if not phone.isdigit() or len(phone) != 11:
                    return {
                        "content": "❌ 请提供有效的11位手机号\n💡 使用方法: /音乐重新登录 手机号",
                        "music_url": None
                    }
                
                self.log_unified("INFO", f"用户 {user_id} 尝试重新登录，手机号: {phone}", group_id="system", user_id=user_id)
                
                # 先尝试账号密码登录
                password_result = self.login_with_password()
                
                if password_result['success']:
                    # 账号密码登录成功
                    return {
                        "content": password_result['message'],
                        "music_url": None
                    }
                else:
                    # 账号密码登录失败，检查是否需要验证码
                    error_msg = password_result.get('message', '')
                    
                    # 如果错误提示包含验证码相关信息，或者是常见的需要验证码的错误码
                    need_captcha = any(keyword in error_msg.lower() for keyword in ['验证码', 'captcha', '需要验证', '安全验证'])
                    
                    if need_captcha or 'error_code' in error_msg:
                        # 存储手机号并发送验证码
                        self.temp_login_phone = phone
                        captcha_result = self.send_verification_code(phone)
                        
                        if captcha_result['success']:
                            return {
                                "content": f"🔐 账号密码登录失败，已发送验证码\n📱 验证码已发送到 {phone}\n💡 请使用: /音乐验证码 收到的验证码\n⏰ 验证码5分钟内有效\n\n🔍 失败原因: {error_msg}",
                                "music_url": None
                            }
                        else:
                            self.temp_login_phone = None  # 清除临时手机号
                            return {
                                "content": f"❌ 账号密码登录失败，验证码发送也失败\n🔍 密码登录失败: {error_msg}\n🔍 验证码发送失败: {captcha_result['message']}",
                                "music_url": None
                            }
                    else:
                        # 其他类型的登录失败，不发送验证码
                        return {
                            "content": f"❌ 账号密码登录失败\n{error_msg}\n💡 请检查config.json中的账号密码配置",
                            "music_url": None
                        }
            
            # 验证码登录指令 - 需要权限验证
            if message.startswith('/音乐验证码') or message.startswith('/music_verify'):
                # 权限检查：只允许指定用户使用验证码登录功能
                if user_id not in self.ALLOWED_RELOGIN_USERS:
                    return {
                        "content": "❌ 权限不足\n💡 提示: 该功能仅限管理员使用",
                        "music_url": None
                    }
                
                if not self.enabled:
                    return {
                        "content": "❌ 音乐服务不可用\n" +
                                  "💡 提示: pyncm库未正确安装，请检查依赖库安装情况",
                        "music_url": None
                    }
                
                # 解析验证码
                parts = message.split()
                if len(parts) < 2:
                    return {
                        "content": "❌ 请提供验证码\n💡 使用方法: /音乐验证码 验证码",
                        "music_url": None
                    }
                
                verification_code = parts[1].strip()
                if not verification_code.isdigit():
                    return {
                        "content": "❌ 验证码应为数字\n💡 使用方法: /音乐验证码 验证码",
                        "music_url": None
                    }
                
                self.log_unified("INFO", f"用户 {user_id} 尝试使用验证码登录，验证码: {verification_code}", group_id="system", user_id=user_id)
                
                # 执行验证码登录
                result = self.login_with_verification_code(verification_code)
                
                # 添加详细的调试信息
                if not result['success'] and 'result' in result:
                    debug_info = f"\n🔍 调试信息: {result['result']}"
                    result['message'] += debug_info
                
                return {
                    "content": result['message'],
                    "music_url": None
                }
            
            # 听歌命令
            if message.startswith('/听歌') or message.startswith('/music'):
                if not self.enabled:
                    return {
                        "content": "❌ 音乐服务暂时不可用，请检查pyncm库是否正确安装",
                        "music_url": None
                    }
                
                parts = message.split(' ', 1)
                if len(parts) < 2 or not parts[1].strip():
                    return {
                        "content": "❌ 请指定歌曲名\n💡 使用方法：/听歌 歌曲名",
                        "music_url": None
                    }
                
                song_name = parts[1].strip()
                
                # 搜索音乐
                search_results = self.search_music(song_name)
                if not search_results:
                    return {
                        "content": f"❌ 未找到歌曲: {song_name}",
                        "music_url": None
                    }
                
                # 获取第一个结果的播放链接
                first_song = search_results[0]
                music_url = self.get_music_url(first_song['id'])
                
                if music_url:
                    # 返回语音消息格式，让napcat直接播放音频
                    return {
                        "content": None,  # 不返回文本内容
                        "voice_message": {
                            "type": "record",
                            "data": {
                                "file": music_url
                            }
                        }
                    }
                else:
                    return {
                        "content": f"❌ 无法获取歌曲播放链接: {song_name}",
                        "music_url": None
                    }
            
            # 搜索音乐命令
            if message.startswith('/搜索音乐') or message.startswith('/search_music'):
                if not self.enabled:
                    return {
                        "content": "❌ 音乐服务暂时不可用，请检查pyncm库是否正确安装",
                        "music_url": None
                    }
                
                parts = message.split(' ', 1)
                if len(parts) < 2 or not parts[1].strip():
                    return {
                        "content": "❌ 请指定搜索关键词\n💡 使用方法：/搜索音乐 关键词",
                        "music_url": None
                    }
                
                keyword = parts[1].strip()
                search_results = self.search_music(keyword, limit=5)
                
                if not search_results:
                    return {
                        "content": f"❌ 未找到相关歌曲: {keyword}",
                        "music_url": None
                    }
                
                result_list = []
                for i, song in enumerate(search_results, 1):
                    song_info = self.format_song_info(song)
                    result_list.append(f"{i}. {song_info}")
                
                content = f"🔍 搜索结果 ({keyword}):\n" + "\n".join(result_list) + "\n\n💡 使用 /听歌 歌曲名 来播放"
                return {
                    "content": content,
                    "music_url": None
                }
            
            return None
            
        except Exception as e:
            self.log_unified("ERROR", f"处理音乐消息失败: {e}", group_id="system", user_id=user_id)
            return {
                "content": f"❌ 处理音乐请求时出现错误: {str(e)}",
                "music_url": None
            }
    
    def get_help_text(self) -> Dict[str, Any]:
        """
        获取帮助文本
        
        Returns:
            Dict[str, Any]: 帮助文本字典
        """
        return {
            "title": "🎵 音乐服务",
            "description": "通过网易云音乐API搜索和播放歌曲",
            "commands": [
                {
                    "command": "/听歌 歌曲名称",
                    "description": "搜索并获取歌曲播放链接",
                    "example": "/听歌 告白气球"
                },
                {
                    "command": "/music 歌曲名称",
                    "description": "搜索并获取歌曲播放链接（英文版）",
                    "example": "/music 告白气球"
                },
                {
                    "command": "/搜索音乐 关键词",
                    "description": "搜索相关歌曲列表",
                    "example": "/搜索音乐 周杰伦"
                },
                {
                    "command": "/音乐源",
                    "description": "查看可用的音乐源",
                    "example": "/音乐源"
                },
                {
                    "command": "/音乐登录状态",
                    "description": "查看网易云音乐登录状态和会员信息",
                    "example": "/音乐登录状态"
                },
                {
                    "command": "/音乐重新登录 手机号",
                    "description": "发送验证码到指定手机号（两步验证登录第一步）",
                    "example": "/音乐重新登录 13812345678"
                },
                {
                    "command": "/音乐验证码 验证码",
                    "description": "使用收到的验证码完成登录（两步验证登录第二步）",
                    "example": "/音乐验证码 123456"
                }
            ],
            "notes": [
                "🎵 支持搜索网易云音乐中的歌曲",
                "🔗 自动获取歌曲播放链接",
                "🔍 支持多首歌曲搜索预览",
                "⚠️ 需要网络连接才能正常使用",
                "💡 部分歌曲可能需要VIP权限",
                "📱 两步验证登录：先发送验证码，再输入验证码完成登录",
                "⏰ 验证码有效期通常为5分钟，请及时使用"
            ]
        }
    
    def get_service_info(self) -> Dict[str, Any]:
        """
        获取服务信息
        
        Returns:
            Dict[str, Any]: 服务信息字典
        """
        return {
            "name": "音乐服务",
            "version": "1.0.0",
            "description": "网络音乐搜索和播放服务",
            "enabled": self.enabled,
            "music_sources": self.get_available_music_sources(),
            "search_limit": self.search_limit,
            "bitrate": self.bitrate
        }