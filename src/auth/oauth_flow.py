"""
Fluxo OAuth automatizado usando Selenium.
Autentica conexões ERP automaticamente sem intervenção manual.
Usa múltiplas estratégias de seleção para resistir a mudanças na página.
"""
from typing import Dict, Any, Optional, List, Tuple
import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse, parse_qs

from src.config.settings import TINY_AUTH_URL, TINY_TOKEN_URL
from src.database.supabase_client import SupabaseClient


def _find_element_resilient(
    driver,
    wait: WebDriverWait,
    strategies: List[Tuple[By, str]],
    timeout_per_try: float = 2.0,
) -> Optional[WebElement]:
    """
    Tenta várias estratégias de localização até encontrar um elemento visível.
    Reduz quebras quando a página muda (novos IDs, classes, estrutura).
    """
    for by, selector in strategies:
        try:
            if timeout_per_try > 0:
                el = wait.until(EC.presence_of_element_located((by, selector)))
            else:
                el = driver.find_element(by, selector)
            if el and el.is_displayed():
                return el
        except (TimeoutException, NoSuchElementException):
            continue
    return None


def _find_button_by_text(
    driver,
    texts: List[str],
    tag: str = "button",
) -> Optional[WebElement]:
    """
    Encontra um botão (ou input submit) cujo texto ou value contenha uma das strings.
    Útil quando a página não usa type=submit ou IDs estáveis.
    """
    text_lower = [t.lower() for t in texts]
    # Botões
    elements = driver.find_elements(By.TAG_NAME, tag)
    for el in elements:
        if not el.is_displayed():
            continue
        raw = (el.text or "") + " " + (el.get_attribute("value") or "")
        if any(t in raw.lower() for t in text_lower):
            return el
    # Inputs type=submit
    if tag == "button":
        for el in driver.find_elements(By.CSS_SELECTOR, "input[type='submit']"):
            if el.is_displayed():
                raw = (el.get_attribute("value") or "").lower()
                if any(t in raw for t in text_lower) or not raw:
                    return el
    return None


class OAuthFlow:
    """Gerencia o fluxo OAuth automatizado com Selenium."""
    
    def __init__(self, db: SupabaseClient):
        self.db = db
    
    def authenticate_connection(self, connection_id: str, erp_type: str = "tiny") -> Dict[str, Any]:
        """
        Autentica uma conexão ERP usando Selenium.
        
        Args:
            connection_id: ID da conexão no banco
            erp_type: Tipo do ERP (padrão: 'tiny')
        
        Returns:
            Dicionário com tokens OAuth
        
        Raises:
            Exception: Se a autenticação falhar
        """
        # Obtém credenciais de acesso ao ERP (login/senha)
        erp_credentials = self.db.get_erp_credentials(connection_id)
        
        # Obtém credenciais OAuth do banco (não mais do .env)
        oauth_credentials = self.db.get_oauth_credentials(connection_id)
        
        # Coleta o code OAuth via Selenium
        code = self._collect_oauth_code(
            erp_credentials["login"],
            erp_credentials["password"],
            oauth_credentials["client_id"],
            oauth_credentials["redirect_uri"]
        )
        
        if not code:
            raise Exception("Não foi possível coletar o código OAuth")
        
        # Troca o code por tokens usando credenciais OAuth do banco
        tokens = self._exchange_code_for_tokens(
            code,
            oauth_credentials["client_id"],
            oauth_credentials["client_secret"],
            oauth_credentials["redirect_uri"]
        )
        
        # Atualiza tokens no banco (são criptografados automaticamente)
        self.db.update_erp_tokens(
            connection_id=connection_id,
            access_token=tokens["access_token"],
            refresh_token=tokens["refresh_token"],
            expires_in=tokens.get("expires_in", 14400),
            refresh_expires_in=tokens.get("refresh_expires_in", 86400)
        )
        
        return tokens
    
    def _collect_oauth_code(self, username: str, password: str, client_id: str, redirect_uri: str) -> Optional[str]:
        """
        Coleta o código OAuth usando automação Selenium.
        
        Args:
            username: Login do ERP
            password: Senha do ERP
        
        Returns:
            Código OAuth ou None em caso de erro
        """
        print("🤖 Iniciando automação Selenium para coletar código OAuth...")
        
        # Configura Chrome em modo headless
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        driver = None
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Monta URL de autorização usando credenciais do banco
            auth_url = (
                f"{TINY_AUTH_URL}?client_id={client_id}&redirect_uri={redirect_uri}&"
                f"scope=openid offline_access&response_type=code"
            )
            
            print(f"🌐 Navegando para página de login...")
            driver.get(auth_url)
            
            wait = WebDriverWait(driver, 30)
            
            # ---- Usuário: várias estratégias ----
            print("✍️  Preenchendo usuário...")
            username_strategies = [
                (By.CSS_SELECTOR, "input[name='username']"),
                (By.CSS_SELECTOR, "input[id='username']"),
                (By.CSS_SELECTOR, "input[type='email']"),
                (By.CSS_SELECTOR, "input[type='text']"),
                (By.XPATH, "//input[@autocomplete='username']"),
                (By.XPATH, "//input[contains(@placeholder, 'mail') or contains(@placeholder, 'usuário') or contains(@placeholder, 'login')]"),
            ]
            username_field = _find_element_resilient(driver, wait, username_strategies)
            if not username_field:
                raise NoSuchElementException("Não foi possível encontrar o campo de usuário/e-mail")
            username_field.clear()
            username_field.send_keys(username)
            time.sleep(1)
            
            # ---- Botão "Avançar" (fluxo em 2 etapas): várias estratégias ----
            avancar_button = _find_button_by_text(driver, ["avançar", "continuar", "next", "próximo"])
            if avancar_button:
                print("🔘 Clicando em Avançar...")
                avancar_button.click()
                time.sleep(2)
            
            # ---- Senha: várias estratégias ----
            print("✍️  Preenchendo senha...")
            password_strategies = [
                (By.CSS_SELECTOR, "input[name='password']"),
                (By.CSS_SELECTOR, "input[id='password']"),
                (By.CSS_SELECTOR, "input[type='password']"),
                (By.XPATH, "//input[@type='password']"),
            ]
            password_field = _find_element_resilient(driver, wait, password_strategies)
            if not password_field:
                raise NoSuchElementException("Não foi possível encontrar o campo de senha")
            password_field.clear()
            password_field.send_keys(password)
            time.sleep(1)
            
            # ---- Botão de login: várias estratégias (XPath Entrar + CSS + texto) ----
            print("🔘 Clicando no botão de login...")
            login_button = None
            # 1) XPath: botão com texto literal "Entrar" (página Tiny / react-login-wc)
            entrar_xpath_strategies = [
                (By.XPATH, "//button[normalize-space(text())='Entrar']"),
                (By.XPATH, "//button[contains(normalize-space(text()), 'Entrar')]"),
                (By.XPATH, "//*[local-name()='react-login-wc']//form//button[contains(text(), 'Entrar')]"),
                (By.XPATH, "//*[local-name()='react-login-wc']//button[normalize-space(text())='Entrar']"),
                (By.XPATH, "//form//button[normalize-space(text())='Entrar']"),
                (By.XPATH, "/html/body/div/div[2]/div/div/react-login-wc/section/main/article/form/button"),
                (By.XPATH, "//react-login-wc//form/button"),
                (By.XPATH, "//button[translate(normalize-space(text()), 'ENTRAR', 'entrar')='entrar']"),
            ]
            login_button = _find_element_resilient(driver, wait, entrar_xpath_strategies, timeout_per_try=1.5)
            # 2) CSS comuns (Keycloak, formulários genéricos)
            if not login_button:
                css_strategies = [
                    (By.CSS_SELECTOR, "button[type='submit']"),
                    (By.CSS_SELECTOR, "input[type='submit']"),
                    (By.CSS_SELECTOR, "button#kc-login"),
                    (By.CSS_SELECTOR, "input#kc-login"),
                    (By.CSS_SELECTOR, "button.btn-primary"),
                    (By.CSS_SELECTOR, "input.btn-primary"),
                    (By.CSS_SELECTOR, "react-login-wc button"),
                    (By.CSS_SELECTOR, "[role='button'][type='submit']"),
                ]
                login_button = _find_element_resilient(driver, wait, css_strategies, timeout_per_try=1.5)
            # 3) Por texto do botão (varre todos os botões)
            if not login_button:
                login_button = _find_button_by_text(driver, ["entrar", "login", "sign in", "acessar", "submit", "enviar"])
            # 4) Qualquer button dentro de form ou react-login-wc
            if not login_button:
                for xpath in ["//form//button", "//*[contains(local-name(), 'react-login')]//button", "//article//form//button"]:
                    try:
                        for el in driver.find_elements(By.XPATH, xpath):
                            if el.is_displayed():
                                login_button = el
                                break
                        if login_button:
                            break
                    except Exception:
                        continue
            # 5) Último recurso: qualquer input submit ou button submit visível
            if not login_button:
                for el in driver.find_elements(By.CSS_SELECTOR, "input[type='submit'], button[type='submit']"):
                    if el.is_displayed():
                        login_button = el
                        break
            if not login_button:
                raise NoSuchElementException(
                    "Não foi possível encontrar o botão de login. "
                    "Tentamos: XPath texto 'Entrar', react-login-wc/form/button, CSS submit, texto do botão."
                )
            login_button.click()
            
            # Aguarda redirecionamento e captura o code
            print("⏳ Aguardando redirecionamento...")
            max_wait = 30
            start_time = time.time()
            
            while time.time() - start_time < max_wait:
                current_url = driver.current_url
                
                # Verifica se foi redirecionado para a redirect_uri com o code
                if redirect_uri.split('?')[0] in current_url and 'code=' in current_url:
                    parsed_url = urlparse(current_url)
                    query_params = parse_qs(parsed_url.query)
                    if 'code' in query_params:
                        code = query_params['code'][0]
                        print(f"✅ Código OAuth coletado com sucesso!")
                        return code
                
                # Verifica se há tela de autorização
                try:
                    authorize_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Autorizar') or contains(text(), 'Allow')]")
                    if authorize_buttons:
                        authorize_buttons[0].click()
                        time.sleep(2)
                        continue
                except:
                    pass
                
                time.sleep(1)
            
            raise TimeoutException("Timeout ao coletar código OAuth")
            
        except Exception as e:
            print(f"❌ Erro durante automação Selenium: {e}")
            raise
        finally:
            if driver:
                driver.quit()
    
    def _exchange_code_for_tokens(self, code: str, client_id: str, client_secret: str, redirect_uri: str) -> Dict[str, Any]:
        """
        Troca o código OAuth por tokens de acesso.
        Usa credenciais OAuth do banco de dados.
        
        Args:
            code: Código OAuth coletado
            client_id: Client ID do banco
            client_secret: Client Secret do banco (já descriptografado)
            redirect_uri: Redirect URI do banco
        
        Returns:
            Dicionário com tokens OAuth
        """
        payload = {
            'grant_type': 'authorization_code',
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri,
            'code': code
        }
        
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = requests.post(TINY_TOKEN_URL, data=payload, headers=headers)
        response.raise_for_status()
        
        return response.json()