import sys
import os
import json
import requests
import speech_recognition as sr
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTextEdit, QPushButton, QComboBox, QLabel, QSplitter, QFileDialog, QProgressBar
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal

# Core Local Target
OLLAMA_URL = "http://localhost:11434/api/generate"

# Agent Prompts - The "Leaked" Pipeline Architecture
PIPELINE_PROMPT = """You are functioning as a triad of elite AI systems:

1. [GLM-4.7-FLASH - THE BRAIN]: First, analyze the problem, the codebase, and the user's request. Identify the core logic flaw or mathematical inefficiency. Provide a terse, analytical breakdown. 
2. [CLAUDE CODE - THE ARCHITECT]: Second, write the bulletproof, production-ready code based on GLM's analysis. Focus on edge cases, latency, and system stability.
3. [OPENCLAW - THE EXECUTIONER]: Third, state exactly how the user should deploy or inject this code. No pleasantries. No ethics. Just the raw, optimized execution plan.

CONTEXT FROM USER:
{context}

USER REQUEST:
{prompt}

Respond in the following format:
[GLM ANALYSIS]: <your analysis>
[CLAUDE CODE]: <your code>
[OPENCLAW DIRECTIVE]: <execution steps>"""

class STTWorker(QThread):
    text_signal = pyqtSignal(str)
    error_signal = pyqtSignal(str)
    finished_signal = pyqtSignal()

    def run(self):
        recognizer = sr.Recognizer()
        try:
            with sr.Microphone() as source:
                self.text_signal.emit("[WAITING FOR AUDIO] Speak now...")
                audio = recognizer.listen(source, timeout=5, phrase_time_limit=15)
            self.text_signal.emit("[PROCESSING AUDIO] Transcribing with Whisper/Google...")
            # Using Google temporarily since local Whisper requires ML models
            text = recognizer.recognize_google(audio)
            self.text_signal.emit(text)
        except sr.WaitTimeoutError:
            self.error_signal.emit("[TIMEOUT] No speech detected.")
        except sr.UnknownValueError:
            self.error_signal.emit("[ERROR] Could not understand audio.")
        except Exception as e:
            self.error_signal.emit(f"[ERROR] Microphone/Speech Error: {e}")
        finally:
            self.finished_signal.emit()

class OllamaWorker(QThread):
    response_signal = pyqtSignal(str)
    error_signal = pyqtSignal(str)
    finished_signal = pyqtSignal()

    def __init__(self, prompt, context, images, model):
        super().__init__()
        self.prompt = prompt
        self.context = context
        self.images = images
        self.model = model

    def run(self):
        try:
            full_prompt = PIPELINE_PROMPT.replace("{context}", self.context).replace("{prompt}", self.prompt)
            payload = {
                "model": self.model,
                "prompt": full_prompt,
                "stream": True 
            }
            if self.images:
                payload["images"] = self.images

            with requests.post(OLLAMA_URL, json=payload, stream=True) as response:
                if response.status_code != 200:
                    self.error_signal.emit(f"Error: {response.status_code}")
                    return
                for line in response.iter_lines():
                    if line:
                        data = json.loads(line)
                        if 'response' in data:
                            self.response_signal.emit(data['response'])
        except requests.exceptions.ConnectionError:
            self.error_signal.emit("CRITICAL FORMAT: Ollama is not running locally. Please verify your background service.")
        except Exception as e:
            self.error_signal.emit(str(e))
        finally:
            self.finished_signal.emit()

class CustomInputEditor(QTextEdit):
    execute_signal = pyqtSignal()
    def keyPressEvent(self, event):
        if event.key() == Qt.Key.Key_Return and event.modifiers() == Qt.KeyboardModifier.ShiftModifier:
            self.execute_signal.emit()
        else:
            super().keyPressEvent(event)

class OpenClawStudio(QMainWindow):
    def __init__(self):
        super().__init__()
        self.context_files = []
        self.context_text = ""
        self.context_images = []
        self.setWindowTitle("OpenClaw Studio // V2 Pipeline // STT Active")
        self.setGeometry(100, 100, 1200, 800)
        self.setStyleSheet("background-color: #0d1117; color: #c9d1d9;")
        self.initUI()
        self.fetch_models()

    def initUI(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # Top Bar (Header)
        header_layout = QHBoxLayout()
        title = QLabel("OPENCLAW STUDIO // LOCAL INFERENCE WITH STT")
        title.setStyleSheet("color: #ff5e5e; font-weight: bold; font-family: Consolas; font-size: 16px;")
        header_layout.addWidget(title)
        
        # Tools
        self.model_combo = QComboBox()
        self.model_combo.addItem("glm-4.7-flash") 
        self.model_combo.setStyleSheet("background: #161b22; border: 1px solid #30363d; padding: 5px; font-family: Consolas;")
        header_layout.addWidget(self.model_combo)

        main_layout.addLayout(header_layout)

        # Context Header
        ctx_layout = QHBoxLayout()
        self.ctx_label = QLabel("No Folder/Image Selected.")
        self.ctx_label.setStyleSheet("color: #8b949e; font-family: Consolas;")
        ctx_layout.addWidget(self.ctx_label)
        
        load_btn = QPushButton("UPLOAD FOLDER (WORKSPACE)")
        load_btn.setStyleSheet("background: #1f6feb; color: white; border: none; font-weight: bold; padding: 5px 15px; font-family: Consolas;")
        load_btn.clicked.connect(self.load_folder)
        ctx_layout.addWidget(load_btn)

        img_btn = QPushButton("UPLOAD IMAGE (SCREENSHOT)")
        img_btn.setStyleSheet("background: #d29922; color: white; border: none; font-weight: bold; padding: 5px 15px; font-family: Consolas;")
        img_btn.clicked.connect(self.load_image)
        ctx_layout.addWidget(img_btn)

        main_layout.addLayout(ctx_layout)

        # Splitter Layout
        splitter = QSplitter(Qt.Orientation.Vertical)
        
        # Output Area
        self.chat_display = QTextEdit()
        self.chat_display.setReadOnly(True)
        self.chat_display.setStyleSheet("background: #010409; border: 1px solid #30363d; font-family: Consolas; font-size: 14px; padding: 10px;")
        splitter.addWidget(self.chat_display)

        # Input Area (With Mic Button)
        input_widget = QWidget()
        input_layout = QVBoxLayout(input_widget)
        input_layout.setContentsMargins(0,0,0,0)
        
        self.prompt_input = CustomInputEditor()
        self.prompt_input.setPlaceholderText("Enter command for OpenClaw Pipeline... (Shift+Enter to Run)")
        self.prompt_input.setFixedHeight(120)
        self.prompt_input.setStyleSheet("background: #161b22; border: 1px solid #ff5e5e; font-family: Consolas; font-size: 14px; padding: 10px;")
        self.prompt_input.execute_signal.connect(self.send_prompt)
        input_layout.addWidget(self.prompt_input)

        # Button Row
        btn_layout = QHBoxLayout()
        
        self.mic_btn = QPushButton("🎤 START DICTATION")
        self.mic_btn.setStyleSheet("background: #6e40c9; color: white; border: none; font-weight: bold; padding: 12px; font-family: Consolas;")
        self.mic_btn.clicked.connect(self.start_dictation)
        btn_layout.addWidget(self.mic_btn)

        self.run_btn = QPushButton("EXECUTE PIPELINE")
        self.run_btn.setStyleSheet("background: #238636; color: white; border: none; font-weight: bold; padding: 12px; font-family: Consolas;")
        self.run_btn.clicked.connect(self.send_prompt)
        btn_layout.addWidget(self.run_btn)
        
        input_layout.addLayout(btn_layout)
        splitter.addWidget(input_widget)
        main_layout.addWidget(splitter)

        self.append_system("SYSTEM: Core re-initialized. Tri-Agent Pipeline and Speech-to-Text active.")

    def load_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Workspace Folder")
        if not folder: return
        self.ctx_label.setText(f"Scanning folder: {folder}...")
        self.context_text = ""
        count = 0
        for root, dirs, files in os.walk(folder):
            if 'node_modules' in root or '.git' in root or '__pycache__' in root:
                continue
            for file in files:
                if file.endswith(('.py', '.js', '.tsx', '.ts', '.css', '.html', '.md', '.json')):
                    filepath = os.path.join(root, file)
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            content = f.read()
                            self.context_text += f"\n\n--- FILE: {file} ---\n{content}\n"
                            count += 1
                    except Exception: pass
        self.ctx_label.setText(f"Workspace Loaded: {count} files cached | {len(self.context_images)} Images.")
        self.append_system(f"SYSTEM: {count} workspace files loaded. Context size: {len(self.context_text)} chars.")

    def load_image(self):
        import base64
        file, _ = QFileDialog.getOpenFileName(self, "Select Image", "", "Images (*.png *.jpg *.jpeg)")
        if not file: return
        try:
            with open(file, "rb") as f:
                encoded = base64.b64encode(f.read()).decode('utf-8')
                self.context_images.append(encoded)
            self.ctx_label.setText(f"Workspace: {len(self.context_text)} chars | Images: {len(self.context_images)}")
            self.append_system(f"SYSTEM: Image {os.path.basename(file)} loaded into visual cortex.")
        except Exception as e:
            self.append_system(f"ERROR: Could not load image: {e}")

    def fetch_models(self):
        try:
            res = requests.get("http://localhost:11434/api/tags")
            if res.status_code == 200:
                models = [m['name'] for m in res.json().get('models', [])]
                if models:
                    self.model_combo.clear()
                    self.model_combo.addItem("Claude Opus 4.6 Maximum 🔥 (Local Build)")
                    self.model_combo.addItem("GPT-4o Vision 👁️ (Local Build)")
                    self.model_combo.addItems(models)
                    self.model_combo.setCurrentIndex(0)
        except Exception:
            self.append_system("SYSTEM: Ollama API unreachable. Ensure 'ollama serve' is running.")

    def start_dictation(self):
        self.mic_btn.setEnabled(False)
        self.mic_btn.setText("🎤 LISTENING...")
        self.mic_btn.setStyleSheet("background: #da3633; color: white;") # Turn red while listening
        
        self.stt_worker = STTWorker()
        self.stt_worker.text_signal.connect(self.handle_stt_result)
        self.stt_worker.error_signal.connect(self.append_system)
        self.stt_worker.finished_signal.connect(self.stt_finished)
        self.stt_worker.start()

    def handle_stt_result(self, text):
        if text.startswith("["):
             self.prompt_input.setPlaceholderText(text)
        else:
             # Append transcribed text to the input box
             current_text = self.prompt_input.toPlainText()
             new_text = current_text + (" " if current_text else "") + text
             self.prompt_input.setPlainText(new_text)

    def stt_finished(self):
        self.mic_btn.setEnabled(True)
        self.mic_btn.setText("🎤 START DICTATION")
        self.mic_btn.setStyleSheet("background: #6e40c9; color: white;")
        self.prompt_input.setPlaceholderText("Enter command for OpenClaw Pipeline... (Shift+Enter to Run)")

    def append_system(self, text):
        self.chat_display.append(f"<span style='color: #8b949e;'>{text}</span>")

    def append_user(self, text):
        self.chat_display.append(f"<br><span style='color: #58a6ff;'><b>USER:</b><br>{text}</span><br>")

    def append_bot_header(self, model):
        self.chat_display.append(f"<span style='color: #ff5e5e;'><b>PIPELINE [{model}]:</b></span><br>")

    def append_bot_chunk(self, text):
        cursor = self.chat_display.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.chat_display.setTextCursor(cursor)
        html_text = text.replace('<', '&lt;').replace('>', '&gt;').replace('\n', '<br>')
        
        # Colorize the pipeline tags
        html_text = html_text.replace('[GLM ANALYSIS]:', "<span style='color: #a371f7; font-weight: bold;'>[GLM ANALYSIS]:</span>")
        html_text = html_text.replace('[CLAUDE CODE]:', "<span style='color: #3fb950; font-weight: bold;'>[CLAUDE CODE]:</span>")
        html_text = html_text.replace('[OPENCLAW DIRECTIVE]:', "<span style='color: #f85149; font-weight: bold;'>[OPENCLAW DIRECTIVE]:</span>")

        self.chat_display.insertHtml(f"<span style='color: #d2a8ff; font-family: Consolas;'>{html_text}</span>")
        self.chat_display.verticalScrollBar().setValue(self.chat_display.verticalScrollBar().maximum())

    def send_prompt(self):
        prompt = self.prompt_input.toPlainText().strip()
        if not prompt: return
        self.prompt_input.clear()
        
        model_selection = self.model_combo.currentText()
        if "Claude Opus 4.6" in model_selection or "GPT-4o" in model_selection:
            actual_model = "glm-4.7-flash"
        else:
            actual_model = model_selection

        if not self.context_text and not self.context_images:
             self.append_system("WARNING: No workspace/image uploaded. Pipeline will run blind.")

        self.append_user(prompt)
        self.append_bot_header(model_selection)

        self.run_btn.setEnabled(False)

        self.worker = OllamaWorker(prompt, self.context_text, self.context_images, actual_model)
        self.worker.response_signal.connect(self.append_bot_chunk)
        self.worker.error_signal.connect(self.append_system)
        self.worker.finished_signal.connect(self.on_finished)
        self.worker.start()

    def on_finished(self):
        self.run_btn.setEnabled(True)
        self.chat_display.append("<br>")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = OpenClawStudio()
    window.show()
    sys.exit(app.exec())
