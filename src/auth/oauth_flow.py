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
import base64
from urllib.parse import urlparse, parse_qs, urlencode

from src.config.settings import (
    TINY_AUTH_URL,
    TINY_TOKEN_URL,
    CONTAZUL_AUTH_URL,
    CONTAZUL_TOKEN_URL,
    CONTAZUL_AUTH_SCOPE,
    BLING_AUTH_URL,
    BLING_TOKEN_URL,
)
from src.database.postgres_client import PostgresClient


def _find_element_resilient(
    driver,
    wait: WebDriverWait,
    strategies: List[Tuple[By, str]],
    timeout_per_try: float = 2.0,
    require_clickable: bool = False,
) -> Optional[WebElement]:
    """
    Tenta várias estratégias de localização até encontrar um elemento visível.
    Reduz quebras quando a página muda (novos IDs, classes, estrutura).
    """
    for by, selector in strategies:
        try:
            if timeout_per_try > 0:
                cond = (
                    EC.element_to_be_clickable((by, selector))
                    if require_clickable
                    else EC.presence_of_element_located((by, selector))
                )
                el = wait.until(cond)
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
    Para input[type=submit], prioriza os que têm value batendo com os textos (ex: "Sign in").
    """
    text_lower = [t.lower() for t in texts]
    # Botões <button>
    elements = driver.find_elements(By.TAG_NAME, tag)
    for el in elements:
        if not el.is_displayed():
            continue
        raw = (el.text or "") + " " + (el.get_attribute("value") or "")
        if any(t in raw.lower() for t in text_lower):
            return el
    # Inputs type=submit: primeiro os que têm value batendo (ex: "Sign in")
    if tag == "button":
        inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='submit']")
        matching = [el for el in inputs if el.is_displayed() and (el.get_attribute("value") or "").strip()]
        for el in matching:
            raw = (el.get_attribute("value") or "").lower()
            if any(t in raw for t in text_lower):
                return el
        for el in inputs:
            if el.is_displayed():
                return el
    return None


class OAuthFlow:
    """Gerencia o fluxo OAuth automatizado com Selenium."""
    
    def __init__(self, db: PostgresClient):
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
        
        # Seleciona URLs e escopo conforme ERP
        if erp_type == "tiny":
            auth_base_url = TINY_AUTH_URL
            token_url = TINY_TOKEN_URL
            scope = "openid offline_access"
        elif erp_type == "contaazul":
            auth_base_url = CONTAZUL_AUTH_URL
            token_url = CONTAZUL_TOKEN_URL
            scope = CONTAZUL_AUTH_SCOPE
        elif erp_type == "bling":
            auth_base_url = BLING_AUTH_URL
            token_url = BLING_TOKEN_URL
            scope = ""  # Bling usa escopos do cadastro do app; redirect_uri/scope opcionais na RFC
        else:
            raise ValueError(f"ERP não suportado para OAuth: {erp_type}")
        
        if not auth_base_url or not token_url:
            raise ValueError(f"URLs OAuth não configuradas para o ERP {erp_type}")
        
        # Coleta o code OAuth via Selenium
        code = self._collect_oauth_code(
            erp_credentials["login"],
            erp_credentials["password"],
            oauth_credentials["client_id"],
            oauth_credentials["redirect_uri"],
            auth_base_url,
            scope,
        )
        
        if not code:
            raise Exception("Não foi possível coletar o código OAuth")
        
        # Troca o code por tokens usando credenciais OAuth do banco
        tokens = self._exchange_code_for_tokens(
            code,
            oauth_credentials["client_id"],
            oauth_credentials["client_secret"],
            oauth_credentials["redirect_uri"],
            token_url,
            erp_type,
        )
        
        # Atualiza tokens no banco (são criptografados automaticamente).
        # Para Conta Azul, o refresh_token deve manter uma janela deslizante de 30 dias:
        # sempre que renovamos (ou fazemos o primeiro auth), empurramos o refresh_expires_at
        # para agora + 30 dias. Assim, só haverá reautenticação se a conexão ficar 30 dias
        # sem nenhuma chamada que force refresh.
        expires_in = tokens.get("expires_in", 14400)
        if erp_type in ("contaazul", "bling"):
            refresh_expires_in = 30 * 24 * 3600  # 30 dias em segundos
        else:
            refresh_expires_in = tokens.get("refresh_expires_in", 86400)

        self.db.update_erp_tokens(
            connection_id=connection_id,
            access_token=tokens["access_token"],
            refresh_token=tokens["refresh_token"],
            expires_in=expires_in,
            refresh_expires_in=refresh_expires_in,
        )
        
        return tokens
    
    def _collect_oauth_code(
        self,
        username: str,
        password: str,
        client_id: str,
        redirect_uri: str,
        auth_base_url: str,
        scope: str,
    ) -> Optional[str]:
        """
        Coleta o código OAuth usando automação Selenium.
        
        Args:
            username: Login do ERP
            password: Senha do ERP
        
        Returns:
            Código OAuth ou None em caso de erro
        """
        print("🤖 Iniciando automação Selenium para coletar código OAuth (modo VISÍVEL)...")
        
        # Configura Chrome em modo NÃO headless para depuração
        chrome_options = Options()
        # chrome_options.add_argument("--headless")  # desativado para permitir visualizar o navegador
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        driver = None
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Monta URL de autorização.
            # Para Conta Azul, seguimos a doc oficial (inclui redirect_uri e scope com + literais).
            # Para Bling, a própria doc informa que redirect_uri/scope são opcionais e usa sempre
            # os valores cadastrados no app. Por isso, na URL enviamos apenas response_type, client_id e state.
            is_bling = "bling.com.br" in auth_base_url.lower()
            is_contaazul = "contaazul" in auth_base_url.lower()
            params_encoded = {
                "response_type": "code",
                "client_id": client_id,
                "state": f"atrio-{int(time.time())}",
            }
            if not is_bling:
                params_encoded["redirect_uri"] = redirect_uri
            query = urlencode(params_encoded)
            # Scope: sem codificar os + (Conta Azul); Bling usa escopos do cadastro do app, omitir se vazio
            if scope:
                query = f"{query}&scope={scope}" if query else f"scope={scope}"
            auth_url = f"{auth_base_url}?{query}"

            print("🌐 URL de autorização construída para OAuth:")
            print(f"    {auth_url}")
            
            print(f"🌐 Navegando para página de login...")
            driver.get(auth_url)
            
            wait = WebDriverWait(driver, 30)
            
            # ---- Usuário: várias estratégias ----
            print("✍️  Preenchendo usuário...")
            username_strategies: List[Tuple[By, str]] = []
            # Bling: página de login específica (xpaths absolutos fornecidos)
            if is_bling:
                username_strategies.append(
                    (By.XPATH, "/html/body/div/div/div/form/div[2]/input")
                )
            username_strategies.extend(
                [
                    (By.CSS_SELECTOR, "input[name='username']"),
                    (By.CSS_SELECTOR, "input[id='username']"),
                    (By.CSS_SELECTOR, "input[type='email']"),
                    (By.CSS_SELECTOR, "input[type='text']"),
                    (By.XPATH, "//input[@autocomplete='username']"),
                    (
                        By.XPATH,
                        "//input[contains(@placeholder, 'mail') "
                        "or contains(@placeholder, 'usuário') "
                        "or contains(@placeholder, 'login')]",
                    ),
                ]
            )
            username_field = _find_element_resilient(driver, wait, username_strategies)
            if not username_field:
                raise NoSuchElementException("Não foi possível encontrar o campo de usuário/e-mail")
            username_field.clear()
            username_field.send_keys(username)
            time.sleep(1)
            
            # ---- Botão "Avançar" apenas em fluxos em 2 etapas (ex.: Tiny). Conta Azul já exibe user e senha juntos. ----
            if "contaazul" not in auth_base_url.lower():
                avancar_button = _find_button_by_text(driver, ["avançar", "continuar", "next", "próximo"])
                if avancar_button:
                    print("🔘 Clicando em Avançar...")
                    avancar_button.click()
                    time.sleep(2)
            
            # ---- Senha: várias estratégias ----
            print("✍️  Preenchendo senha...")
            password_strategies: List[Tuple[By, str]] = []
            if is_bling:
                password_strategies.append(
                    (By.XPATH, "/html/body/div/div/div/form/div[3]/input")
                )
            password_strategies.extend(
                [
                    (By.CSS_SELECTOR, "input[name='password']"),
                    (By.CSS_SELECTOR, "input[id='password']"),
                    (By.CSS_SELECTOR, "input[type='password']"),
                    (By.XPATH, "//input[@type='password']"),
                ]
            )
            password_field = _find_element_resilient(driver, wait, password_strategies)
            if not password_field:
                raise NoSuchElementException("Não foi possível encontrar o campo de senha")
            password_field.clear()
            password_field.send_keys(password)
            time.sleep(1)
            
            # ---- Botão de login: sempre buscar Sign in OU Entrar (página pode traduzir) ----
            print("🔘 Clicando no botão de login...")
            login_button = None
            # 1) Sign in ou Entrar — input value ou texto do botão (Conta Azul / Tiny / Bling / tradução)
            signin_entrar_strategies: List[Tuple[By, str]] = []
            if is_bling:
                signin_entrar_strategies.append(
                    (By.XPATH, "/html/body/div/div/div/form/div[6]/button")
                )
            # Tiny/Conta Azul: prioriza seletores estáveis do DOM
            # (reduz o número de tentativas e evita esperar o timeout por muitos seletores).
            if is_contaazul:
                # Conta Azul: o botão costuma estar dentro de um modal específico.
                # Seu seletor (querySelector) e XPath absoluto entram como prioridade máxima.
                signin_entrar_strategies.extend(
                    [
                        (
                            By.CSS_SELECTOR,
                            "input.btn.btn-primary.submitButton-customizable",
                        ),
                        (
                            By.XPATH,
                            "/html/body/div[1]/div/div[1]/div[2]/div[2]/div[3]/div/div/form/input[3]",
                        ),
                    ]
                )
            elif not is_bling:
                signin_entrar_strategies.extend(
                    [
                        (By.CSS_SELECTOR, "#input-wrapper > button"),
                        (
                            By.XPATH,
                            "/html/body/div/div[2]/div/div/react-login-wc/section/main/article/form/button",
                        ),
                        (By.CSS_SELECTOR, "react-login-wc button"),
                    ]
                )
            signin_entrar_strategies.extend(
                [
                    (
                        By.XPATH,
                        "//input[@type='submit' and "
                        "(@value='Sign in' or @value='Sign In' or @value='Entrar')]",
                    ),
                    (
                        By.XPATH,
                        "//input[contains(@value,'Sign in') "
                        "or contains(@value,'Sign In') "
                        "or contains(@value,'Entrar')]",
                    ),
                    (
                        By.XPATH,
                        "//button[normalize-space(text())='Sign in' "
                        "or normalize-space(text())='Sign In' "
                        "or normalize-space(text())='Entrar']",
                    ),
                    (
                        By.XPATH,
                        "//button[contains(normalize-space(text()), 'Entrar') "
                        "or contains(normalize-space(text()), 'Sign in')]",
                    ),
                    (By.CSS_SELECTOR, "input[type='submit'][value='Sign in']"),
                    (By.CSS_SELECTOR, "input[type='submit'][value='Sign In']"),
                    (By.CSS_SELECTOR, "input[type='submit'][value='Entrar']"),
                    (By.CSS_SELECTOR, "input.btn-primary[value='Sign in']"),
                    (By.CSS_SELECTOR, "input.submitButton-customizable"),
                    (By.CSS_SELECTOR, "form input.btn.btn-primary"),
                    (
                        By.XPATH,
                        "//*[local-name()='react-login-wc']//form//button[contains(text(), 'Entrar')]",
                    ),
                    (By.XPATH, "//form//button[normalize-space(text())='Entrar']"),
                    (By.XPATH, "//react-login-wc//form/button"),
                ]
            )
            login_button = _find_element_resilient(
                driver,
                wait,
                signin_entrar_strategies,
                timeout_per_try=0.8 if is_contaazul else 1.5,
                # Em alguns fluxos do Conta Azul, o elemento pode existir mas ficar
                # momentaneamente "não clicável" (covered/spinner). Para reduzir tempo
                # de espera, usamos a validação de presença (não estritamente clicável).
                require_clickable=not is_contaazul,
            )
            # 2) Por texto/value do botão (Sign in e Entrar com mesma prioridade)
            if not login_button:
                login_button = _find_button_by_text(driver, ["sign in", "entrar", "login", "acessar", "submit", "enviar"])
            # 3) CSS comuns (Keycloak, formulários genéricos)
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
                    "Não foi possível encontrar o botão de login (Sign in / Entrar / submit)."
                )
            # Clique robusto: garante scroll e tenta JS click como fallback.
            try:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", login_button
                )
                time.sleep(0.2)
                login_button.click()
            except Exception:
                driver.execute_script("arguments[0].click();", login_button)
            
            # Aguarda redirecionamento e captura o code.
            # Para Bling, não há etapa de código autenticador (2FA/MFA) neste fluxo,
            # então apenas aguardamos o redirect normalmente com um timeout menor.
            if is_bling:
                print("⏳ Aguardando redirecionamento do Bling...")
                max_wait = 60  # tempo suficiente para redirect simples
            else:
                print("⏳ Aguardando redirecionamento (incluindo eventual passo de 2FA/MFA)...")
                print("   Se a tela pedir um código do autenticador, digite-o no navegador; vamos aguardar alguns minutos.")
                max_wait = 180  # até 3 minutos para o usuário confirmar MFA
            start_time = time.time()
            # Regra de detecção do redirect:
            # - Se o browser estiver na mesma origem (scheme+host) do redirect_uri
            # - e existir `code` na URL atual
            # então consideramos que encontramos o code.
            redirect_base = ""
            try:
                parsed_redirect = urlparse(redirect_uri)
                redirect_base = f"{parsed_redirect.scheme}://{parsed_redirect.netloc}"
            except Exception:
                redirect_base = redirect_uri.split("?")[0]
            
            while time.time() - start_time < max_wait:
                current_url = driver.current_url
                
                # Para Bling, é comum o usuário informar um redirect_uri "não canônico"
                # (ex.: URL do endpoint de authorize em vez da callback real).
                # Então, para Bling, detectamos apenas pela existência do `code=` na URL.
                if "code=" in current_url and (
                    is_bling or (redirect_base and redirect_base in current_url)
                ):
                    parsed_url = urlparse(current_url)
                    # Alguns provedores retornam parâmetros no fragment (#...)
                    query_params = parse_qs(parsed_url.query)
                    fragment_params = parse_qs(parsed_url.fragment)
                    code_params = query_params if "code" in query_params else fragment_params
                    if "code" in code_params and code_params["code"]:
                        code = code_params["code"][0]
                        print("✅ Código OAuth coletado com sucesso!")
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
    
    def _exchange_code_for_tokens(
        self,
        code: str,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        token_url: str,
        erp_type: str,
    ) -> Dict[str, Any]:
        """
        Troca o código OAuth por tokens de acesso.
        Usa credenciais OAuth do banco de dados.
        Bling exige autenticação HTTP Basic (client_id:client_secret em base64 no header).
        
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
            'redirect_uri': redirect_uri,
            'code': code
        }
        if erp_type != "bling":
            payload['client_id'] = client_id
            payload['client_secret'] = client_secret

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        if erp_type == "bling":
            basic_creds = base64.b64encode(
                f"{client_id}:{client_secret}".encode()
            ).decode()
            headers['Authorization'] = f"Basic {basic_creds}"

        response = requests.post(token_url, data=payload, headers=headers)
        response.raise_for_status()

        return response.json()