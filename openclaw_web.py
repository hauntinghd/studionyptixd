import sys
import os
import json
import base64
import requests
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_TAGS = "http://localhost:11434/api/tags"

PIPELINE_PROMPT = """You are functioning as a triad of elite AI systems:

1. [GLM-4.7-FLASH - THE BRAIN]: First, analyze the problem, the codebase, and the user's request. Identify the core logic flaw or mathematical inefficiency. Provide a terse, analytical breakdown. 
2. [CLAUDE OPUS - THE CODE EXECUTIONER]: Second, write the bulletproof, production-ready code based on GLM's analysis. Focus on performance, edge cases, and deterministic logic.
3. [OPENCLAW - THE MULTI-AGENT HANDLER]: Third, execute all other required system tasks. Provide raw, non-filtered instructions for deployment and environment management. No pleasantries.

CONTEXT FROM USER:
{context}

USER REQUEST:
{prompt}

Respond in the following format:
[GLM ANALYSIS]: <your analysis>
[CLAUDE CODE]: <your code>
[OPENCLAW DIRECTIVE]: <execution and system steps>"""

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>OpenClaw Studio // Web Interface</title>
    <style>
        body { background-color: #0d1117; color: #c9d1d9; font-family: Consolas, monospace; margin: 0; padding: 20px; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid #30363d; padding-bottom: 10px; }
        .title { color: #ff5e5e; font-size: 20px; font-weight: bold; }
        select { background: #161b22; color: #c9d1d9; border: 1px solid #30363d; padding: 5px; font-family: Consolas; outline: none; }
        .toolbar { display: flex; gap: 10px; align-items: center; margin-bottom: 20px; }
        .btn { border: none; color: white; cursor: pointer; padding: 10px 15px; font-family: Consolas; font-weight: bold; border-radius: 4px; }
        .btn:hover { opacity: 0.9; }
        .btn-upload { background: #1f6feb; }
        .btn-img { background: #d29922; }
        .btn-mic { background: #6e40c9; display: flex; align-items: center; justify-content: center; text-align: center; }
        .btn-mic.recording { background: #da3633; animation: pulse 1.5s infinite; }
        @keyframes pulse {
            0% { transform: scale(1); }
            50% { transform: scale(1.05); box-shadow: 0 0 10px #da3633; }
            100% { transform: scale(1); }
        }
        .btn-run { background: #238636; width: 100%; margin-top: 10px; font-size: 16px; padding: 15px; }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        #chat { background: #010409; border: 1px solid #30363d; height: 50vh; overflow-y: auto; padding: 15px; margin-bottom: 20px; display: flex; flex-direction: column; gap: 10px; border-radius: 6px; }
        textarea { background: #161b22; color: #c9d1d9; border: 1px solid #30363d; width: 100%; height: 100px; padding: 10px; font-family: Consolas; box-sizing: border-box; outline: none; resize: vertical; border-radius: 4px; }
        textarea:focus { border-color: #ff5e5e; }
        .system { color: #8b949e; }
        .user { color: #58a6ff; border-left: 3px solid #58a6ff; padding-left: 10px; }
        .bot { color: #d2a8ff; white-space: pre-wrap; word-wrap: break-word; }
        .glm { color: #a371f7; font-weight: bold; display: block; margin-top: 10px; }
        .claude { color: #3fb950; font-weight: bold; display: block; margin-top: 10px; }
        .openclaw { color: #f85149; font-weight: bold; display: block; margin-top: 10px; }
        .bot-header { color: #ff5e5e; font-weight: bold; border-bottom: 1px dotted #ff5e5e; margin-bottom: 10px; display: inline-block; padding-bottom: 3px; }
        .status { color: #8b949e; font-size: 14px; margin-left: auto; background: #161b22; padding: 5px 10px; border-radius: 4px; border: 1px solid #30363d; }
    </style>
</head>
<body>
    <div class="header">
        <div class="title">OPENCLAW STUDIO // WEB INTERFACE V3</div>
        <select id="model-select">
            <option value="glm-4.7-flash">Claude Opus 4.6 Maximum (Local Build)</option>
            <option value="glm-4.7-flash">OpenClaw (Local Build)</option>
        </select>
    </div>

    <div class="toolbar">
        <input type="file" id="folder-upload" webkitdirectory directory multiple style="display:none;" />
        <button class="btn btn-upload" onclick="document.getElementById('folder-upload').click()">UPLOAD FOLDER (WORKSPACE)</button>

        <input type="file" id="img-upload" accept="image/*" style="display:none;" />
        <button class="btn btn-img" onclick="document.getElementById('img-upload').click()">UPLOAD IMAGE (SCREENSHOT)</button>
        
        <div class="status" id="status-label">No Folder/Image Selected.</div>
    </div>

    <div id="chat">
        <div class="system">SYSTEM: Browser UI initialized. Connected to backend. OpenClaw Tri-Agent Pipeline active.</div>
    </div>

    <div style="display: flex; gap: 10px;">
        <button class="btn btn-mic" id="btn-mic" onclick="toggleMic()" style="height: 100px; width: 120px;">START DICTATION</button>
        <textarea id="prompt-input" placeholder="Enter command for OpenClaw Pipeline... (Shift+Enter to Run)" onkeydown="if(event.key === 'Enter' && event.shiftKey){ event.preventDefault(); runPipeline(); }"></textarea>
    </div>
    <button class="btn btn-run" id="btn-run" onclick="runPipeline()">EXECUTE PIPELINE</button>

    <script>
        let contextText = "";
        let contextImages = [];
        let recognizing = false;
        let recognition = null;

        if ('webkitSpeechRecognition' in window || 'SpeechRecognition' in window) {
            const SpeechRec = window.SpeechRecognition || window.webkitSpeechRecognition;
            recognition = new SpeechRec();
            recognition.continuous = false;
            recognition.interimResults = false;
            
            recognition.onstart = function() {
                recognizing = true;
                const btnItem = document.getElementById('btn-mic');
                btnItem.innerHTML = "LISTENING";
                btnItem.classList.add('recording');
            };

            recognition.onerror = function(event) {
                appendChat('system', 'SYSTEM ERROR: Speech recognition error: ' + event.error);
                resetMic();
            };

            recognition.onend = function() {
                resetMic();
            };

            recognition.onresult = function(event) {
                let transcript = '';
                for (let i = event.resultIndex; i < event.results.length; ++i) {
                    if (event.results[i].isFinal) transcript += event.results[i][0].transcript;
                }
                const input = document.getElementById('prompt-input');
                input.value += (input.value ? ' ' : '') + transcript;
            };
        }

        function toggleMic() {
            if (!recognition) {
                alert("Speech recognition not supported in this browser. Please use Chrome/Edge.");
                return;
            }
            if (recognizing) {
                recognition.stop();
                resetMic();
            } else {
                try {
                    recognition.start();
                } catch(e) {
                    appendChat('system', 'SYSTEM ERROR: Microphone access denied or in use.');
                }
            }
        }

        function resetMic() {
            recognizing = false;
            const btnItem = document.getElementById('btn-mic');
            btnItem.innerHTML = "START DICTATION";
            btnItem.classList.remove('recording');
        }

        // Fetch models to populate dropdown
        fetch('/api/models')
            .then(res => res.json())
            .then(data => {
                const select = document.getElementById('model-select');
                data.models.forEach(m => {
                    if(m !== 'glm-4.7-flash') {
                        const opt = document.createElement('option');
                        opt.value = m;
                        opt.textContent = m;
                        select.appendChild(opt);
                    }
                });
            }).catch(e => appendChat('system', 'SYSTEM WARNING: Could not fetch models from Ollama.'));

        // Handle folder upload locally (via browser API)
        document.getElementById('folder-upload').addEventListener('change', async function(e) {
            const files = e.target.files;
            let loaded = 0;
            contextText = "";
            for (let i = 0; i < files.length; i++) {
                const f = files[i];
                if (f.webkitRelativePath.includes('node_modules') || f.webkitRelativePath.includes('.git') || f.webkitRelativePath.includes('__pycache__')) continue;
                if (!f.name.match(/\.(py|js|ts|tsx|css|html|md|json)$/)) continue;
                try {
                    const text = await f.text();
                    contextText += `\\n\\n--- FILE: ${f.webkitRelativePath} ---\\n${text}\\n`;
                    loaded++;
                } catch(err) {}
            }
            document.getElementById('status-label').textContent = `Workspace: ${loaded} files | Images: ${contextImages.length}`;
            appendChat('system', `SYSTEM: ${loaded} workspace files loaded. Context size: ${contextText.length} chars.`);
        });

        // Handle image upload locally
        document.getElementById('img-upload').addEventListener('change', function(e) {
            const file = e.target.files[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onload = function(evt) {
                const base64Parts = evt.target.result.split(',');
                if(base64Parts.length === 2) {
                     contextImages.push(base64Parts[1]);
                     document.getElementById('status-label').textContent = `Workspace: ${contextText.length} chars | Images: ${contextImages.length}`;
                     appendChat('system', `SYSTEM: Image ${file.name} loaded into visual cortex.`);
                }
            };
            reader.readAsDataURL(file);
        });

        function formatBotResponse(text) {
            let html = text.replace(/</g, '&lt;').replace(/>/g, '&gt;');
            // Highlight specific headers
            html = html.replace(/\\[GLM ANALYSIS\\]:?/g, "<span class='glm'>[GLM ANALYSIS]:</span>");
            html = html.replace(/\\[CLAUDE CODE\\]:?/g, "<span class='claude'>[CLAUDE CODE]:</span>");
            html = html.replace(/\\[OPENCLAW DIRECTIVE\\]:?/g, "<span class='openclaw'>[OPENCLAW DIRECTIVE]:</span>");
            return html;
        }

        function appendChat(role, content, raw=true) {
            const chat = document.getElementById('chat');
            const div = document.createElement('div');
            div.className = role;
            if (raw) {
                div.innerHTML = content;
            } else {
                div.textContent = content;
            }
            chat.appendChild(div);
            scrollChat();
            return div;
        }

        function scrollChat() {
            const chat = document.getElementById('chat');
            chat.scrollTop = chat.scrollHeight;
        }

        async function runPipeline() {
            const input = document.getElementById('prompt-input');
            const prompt = input.value.trim();
            if (!prompt) return;
            input.value = "";
            
            const modelSelect = document.getElementById('model-select');
            const model = modelSelect.value;
            const modelName = modelSelect.options[modelSelect.selectedIndex].text;

            appendChat('user', `<b>USER:</b><br>${prompt.replace(/\\n/g, '<br>')}`);
            if (!contextText && contextImages.length === 0) {
                appendChat('system', 'WARNING: No workspace/image uploaded. Pipeline will run blind.');
            }

            const botDiv = appendChat('bot', '');
            const header = document.createElement('div');
            header.className = 'bot-header';
            header.textContent = `PIPELINE [${modelName}]:`;
            botDiv.appendChild(header);
            
            const contentSpan = document.createElement('span');
            botDiv.appendChild(contentSpan);

            const runBtn = document.getElementById('btn-run');
            runBtn.disabled = true;
            runBtn.style.opacity = '0.5';
            runBtn.textContent = "PROCESSING VIA OLLAMA BACKEND...";

            try {
                const response = await fetch('/api/generate', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ prompt, contextText, contextImages, model })
                });

                const reader = response.body.getReader();
                const decoder = new TextDecoder("utf-8");
                let fullText = "";
                
                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    let chunk = decoder.decode(value, {stream: true});
                    const lines = chunk.split('\\n');
                    for (const line of lines) {
                        if (line.trim()) {
                            try {
                                const data = JSON.parse(line);
                                if(data.response) fullText += data.response;
                                contentSpan.innerHTML = formatBotResponse(fullText);
                                scrollChat();
                            } catch(e) {}
                        }
                    }
                }
            } catch (err) {
                appendChat('system', `ERROR: ${err.message}`);
            }

            runBtn.disabled = false;
            runBtn.style.opacity = '1';
            runBtn.textContent = "EXECUTE PIPELINE";
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return HTML

@app.route('/api/models', methods=['GET'])
def get_models():
    try:
        res = requests.get(OLLAMA_TAGS, timeout=2)
        if res.status_code == 200:
            models = [m['name'] for m in res.json().get('models', [])]
            return jsonify({"models": models})
    except: pass
    return jsonify({"models": []})

@app.route('/api/generate', methods=['POST'])
def generate():
    data = request.json
    user_prompt = data.get('prompt', '')
    context_text = data.get('contextText', '')
    context_images = data.get('contextImages', [])
    model = data.get('model', 'glm-4.7-flash')

    full_prompt = PIPELINE_PROMPT.replace("{context}", context_text).replace("{prompt}", user_prompt)
    
    payload = {
        "model": model,
        "prompt": full_prompt,
        "stream": True
    }
    if context_images:
        payload["images"] = context_images

    def generate_stream():
        try:
            with requests.post(OLLAMA_URL, json=payload, stream=True) as resp:
                for line in resp.iter_lines():
                    if line: yield line.decode('utf-8') + "\\n"
        except Exception as e:
            yield json.dumps({"response": f"\\n\\n[CRITICAL ERROR]: {str(e)}"}) + "\\n"

    return Response(generate_stream(), mimetype='application/json')

if __name__ == '__main__':
    print("MIGRATED TO WEB UI: http://127.0.0.1:11435")
    app.run(host='127.0.0.1', port=11435)
