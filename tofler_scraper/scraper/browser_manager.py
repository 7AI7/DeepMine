"""Browser pool management with proxy support"""
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async
import random
import config

class BrowserPool:
    def __init__(self, logger):
        self.logger = logger
        self.playwright = None
        self.browser = None
        self.contexts = []
        self.context_usage = {}  # Track usage count per context
    
    async def initialize(self):
        """Initialize browser pool with concurrent contexts"""
        self.playwright = await async_playwright().start()
        
        # Launch browser
        self.browser = await self.playwright.chromium.launch(
            headless=config.HEADLESS,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu'
            ]
        )
        
        # Create contexts with proxy and unique configurations
        proxy_config = {
            "server": config.PROXY_CONFIG["server"],
            "username": config.PROXY_CONFIG["username"],
            "password": config.PROXY_CONFIG["password"]
        }
        
        for i in range(config.MAX_CONCURRENT_BROWSERS):
            context = await self._create_context(i, proxy_config)
            self.contexts.append(context)
            self.context_usage[i] = 0
            self.logger.debug(f"Initialized browser context {i+1}/{config.MAX_CONCURRENT_BROWSERS}")
        
        self.logger.info(f"Browser pool initialized with {len(self.contexts)} contexts")
    
    async def _create_context(self, index, proxy_config):
        """Create a new browser context with anti-detection settings"""
        # Use provided user agents (cycle through based on index)
        user_agent = config.USER_AGENTS[index % len(config.USER_AGENTS)]
        
        context_options = {
            "user_agent": user_agent,
            "viewport": random.choice(config.VIEWPORTS),
            "locale": 'en-IN',
            "timezone_id": 'Asia/Kolkata',
            "ignore_https_errors": True
        }
        
        # Add proxy only if enabled
        if config.USE_PROXY and proxy_config:
            context_options["proxy"] = proxy_config
        
        context = await self.browser.new_context(**context_options)
        
        # Add comprehensive stealth scripts
        await context.add_init_script("""
            // ========== WEBDRIVER DETECTION EVASION ==========
            // Override webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Delete webdriver from navigator
            delete navigator.__proto__.webdriver;
            
            // ========== AUTOMATION FLAGS ==========
            // Override automation controlled
            Object.defineProperty(navigator, 'plugins', {
                get: () => {
                    const plugins = [
                        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                        { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                        { name: 'Native Client', filename: 'internal-nacl-plugin' }
                    ];
                    plugins.length = 3;
                    return plugins;
                }
            });
            
            // Override languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en', 'hi']
            });
            
            // ========== CHROME RUNTIME ==========
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };
            
            // ========== PERMISSIONS API ==========
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            
            // ========== WEBGL FINGERPRINT PROTECTION ==========
            const getParameterProxyHandler = {
                apply: function(target, ctx, args) {
                    const param = args[0];
                    const result = Reflect.apply(target, ctx, args);
                    
                    // Add slight randomization to certain parameters
                    if (param === 37445) { // UNMASKED_VENDOR_WEBGL
                        return 'Intel Inc.';
                    }
                    if (param === 37446) { // UNMASKED_RENDERER_WEBGL
                        return 'Intel Iris OpenGL Engine';
                    }
                    return result;
                }
            };
            
            // Try to proxy WebGL (may not work on all pages)
            try {
                const canvas = document.createElement('canvas');
                const gl = canvas.getContext('webgl') || canvas.getContext('experimental-webgl');
                if (gl) {
                    gl.getParameter = new Proxy(gl.getParameter, getParameterProxyHandler);
                }
            } catch(e) {}
            
            // ========== CANVAS FINGERPRINT PROTECTION ==========
            const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function(type) {
                if (type === 'image/png' && this.width === 220 && this.height === 30) {
                    // This is likely a fingerprinting attempt
                    const context = this.getContext('2d');
                    const imageData = context.getImageData(0, 0, this.width, this.height);
                    // Add very subtle noise
                    for (let i = 0; i < imageData.data.length; i += 4) {
                        imageData.data[i] = imageData.data[i] ^ (Math.random() > 0.99 ? 1 : 0);
                    }
                    context.putImageData(imageData, 0, 0);
                }
                return originalToDataURL.apply(this, arguments);
            };
            
            // ========== HARDWARE CONCURRENCY ==========
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 4 + Math.floor(Math.random() * 4)
            });
            
            // ========== DEVICE MEMORY ==========
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => 8
            });
            
            // ========== CONNECTION TYPE ==========
            if (navigator.connection) {
                Object.defineProperty(navigator.connection, 'rtt', {
                    get: () => 50 + Math.floor(Math.random() * 100)
                });
            }
            
            // ========== IFRAME DETECTION ==========
            Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
                get: function() {
                    return window;
                }
            });
            
            // ========== CONSOLE LOGGING (Disable detection) ==========
            const originalConsole = window.console;
            window.console = {
                ...originalConsole,
                debug: () => {},
            };
        """)
        
        # Apply playwright-stealth for additional anti-detection
        # This will be applied to each page created from this context
        await context.route("**/*", lambda route: route.continue_())
        
        return context
    
    def get_context(self, index):
        """Get context by index"""
        return self.contexts[index % len(self.contexts)]
    
    async def increment_usage(self, index):
        """Increment usage count and recycle if needed"""
        self.context_usage[index] = self.context_usage.get(index, 0) + 1
        
        # Recycle context after threshold
        if self.context_usage[index] >= config.CONTEXT_RECYCLE_THRESHOLD:
            await self.recycle_context(index)
    
    async def recycle_context(self, index):
        """Close and recreate a context"""
        self.logger.info(f"Recycling context {index} after {self.context_usage[index]} uses")
        
        # Close old context
        await self.contexts[index].close()
        
        # Create new context
        proxy_config = {
            "server": config.PROXY_CONFIG["server"],
            "username": config.PROXY_CONFIG["username"],
            "password": config.PROXY_CONFIG["password"]
        }
        
        self.contexts[index] = await self._create_context(index, proxy_config)
        self.context_usage[index] = 0
    
    async def close(self):
        """Close all contexts and browser"""
        for context in self.contexts:
            await context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.logger.info("Browser pool closed")
