from openai import OpenAI
import re
from dotenv import load_dotenv
import os

class GPTToolExtractor:
    def __init__(self, time, input_list):
        load_dotenv()
        self.input_list = input_list
        self.time = time
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.result = self.gpt_tools()
    
    def process_markdown_content(self, markdown_text):
        """Process markdown and return structured content for injection"""
        lines = markdown_text.strip().split("\n")
        processed_lines = []
        
        for line in lines:
            line = line.strip()
            if line:
                if line.startswith("### "):
                    processed_lines.append(f'<h3 class="section-subtitle">{line[4:]}</h3>')
                elif line.startswith("## "):
                    processed_lines.append(f'<h2 class="section-title">{line[3:]}</h2>')
                elif line.startswith("# "):
                    processed_lines.append(f'<h1 class="main-title">{line[2:]}</h1>')
                elif line.startswith("- "):
                    # Handle bullet points
                    processed_lines.append(f'<li class="bullet-item">{line[2:]}</li>')
                elif line.startswith("*"):
                    # Handle asterisk bullets
                    processed_lines.append(f'<li class="bullet-item">{line[1:].strip()}</li>')
                else:
                    # Regular paragraph
                    if not line.startswith('<'):
                        processed_lines.append(f'<p class="content-text">{line}</p>')
        
        # Wrap consecutive <li> elements in <ul>
        final_content = []
        in_list = False
        
        for line in processed_lines:
            if line.startswith('<li'):
                if not in_list:
                    final_content.append('<ul class="content-list">')
                    in_list = True
                final_content.append(line)
            else:
                if in_list:
                    final_content.append('</ul>')
                    in_list = False
                final_content.append(line)
        
        if in_list:
            final_content.append('</ul>')
        
        return '\n'.join(final_content)

    def create_beautiful_html(self, content):
        """Create a beautiful, modern HTML page with PDF download functionality"""
        
        return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Learning Roadmap - {', '.join(self.input_list)}</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/html2pdf.js/0.10.1/html2pdf.bundle.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
            color: #2c3e50;
        }}
        
        .container {{
            max-width: 900px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            overflow: hidden;
            box-shadow: 0 20px 60px rgba(0,0,0,0.1);
            position: relative;
        }}
        
        .header {{
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            padding: 40px 30px;
            text-align: center;
            color: white;
            position: relative;
        }}
        
        .header::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 20"><defs><radialGradient id="a" cx="50%" cy="40%"><stop offset="0%" stop-color="%23ffffff" stop-opacity="0.1"/><stop offset="100%" stop-color="%23ffffff" stop-opacity="0"/></radialGradient></defs><rect width="100" height="20" fill="url(%23a)"/></svg>');
            opacity: 0.3;
        }}
        
        .roadmap-title {{
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 15px;
            text-shadow: 0 2px 4px rgba(0,0,0,0.1);
            position: relative;
            z-index: 1;
        }}
        
        .roadmap-subtitle {{
            font-size: 1.2rem;
            opacity: 0.9;
            font-weight: 300;
            position: relative;
            z-index: 1;
        }}
        
        .download-section {{
            background: #f8f9fa;
            padding: 20px 30px;
            border-bottom: 1px solid #e9ecef;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 15px;
        }}
        
        .download-info {{
            display: flex;
            align-items: center;
            gap: 15px;
            color: #6c757d;
            font-size: 0.9rem;
        }}
        
        .download-btn {{
            background: linear-gradient(135deg, #ff6b6b, #ee5a24);
            color: white;
            border: none;
            padding: 12px 25px;
            border-radius: 25px;
            cursor: pointer;
            font-weight: 600;
            font-size: 0.9rem;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(255,107,107,0.3);
        }}
        
        .download-btn:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(255,107,107,0.4);
        }}
        
        .content {{
            padding: 40px 30px;
            line-height: 1.8;
        }}
        
        .main-title {{
            color: #2c3e50;
            font-size: 2rem;
            font-weight: 700;
            margin: 40px 0 20px 0;
            padding-bottom: 15px;
            border-bottom: 3px solid #4facfe;
            position: relative;
        }}
        
        .main-title:first-child {{
            margin-top: 0;
        }}
        
        .section-title {{
            color: #34495e;
            font-size: 1.5rem;
            font-weight: 600;
            margin: 30px 0 15px 0;
            padding-left: 20px;
            border-left: 4px solid #00f2fe;
            background: linear-gradient(90deg, rgba(79,172,254,0.1) 0%, rgba(0,242,254,0.05) 100%);
            padding: 15px 20px;
            border-radius: 8px;
        }}
        
        .section-subtitle {{
            color: #5a6c7d;
            font-size: 1.2rem;
            font-weight: 600;
            margin: 25px 0 12px 0;
            padding-left: 15px;
            border-left: 3px solid #a29bfe;
        }}
        
        .content-list {{
            margin: 15px 0 25px 20px;
            padding-left: 20px;
        }}
        
        .bullet-item {{
            margin: 8px 0;
            padding: 8px 12px;
            background: rgba(116, 185, 255, 0.05);
            border-radius: 6px;
            border-left: 3px solid #74b9ff;
            transition: all 0.2s ease;
        }}
        
        .bullet-item:hover {{
            background: rgba(116, 185, 255, 0.1);
            transform: translateX(5px);
        }}
        
        .content-text {{
            margin: 12px 0;
            color: #2c3e50;
            font-size: 1rem;
        }}
        
        .progress-indicator {{
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 4px;
            background: rgba(255,255,255,0.2);
            z-index: 1000;
        }}
        
        .progress-bar {{
            height: 100%;
            background: linear-gradient(90deg, #4facfe, #00f2fe);
            width: 0%;
            transition: width 0.3s ease;
        }}
        
        .floating-nav {{
            position: fixed;
            right: 30px;
            top: 50%;
            transform: translateY(-50%);
            background: white;
            border-radius: 15px;
            padding: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            z-index: 100;
        }}
        
        .nav-item {{
            display: block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #ddd;
            margin: 8px 0;
            cursor: pointer;
            transition: all 0.3s ease;
        }}
        
        .nav-item.active {{
            background: #4facfe;
            transform: scale(1.2);
        }}
        
        @media (max-width: 768px) {{
            .container {{
                margin: 10px;
                border-radius: 15px;
            }}
            
            .header {{
                padding: 30px 20px;
            }}
            
            .roadmap-title {{
                font-size: 2rem;
            }}
            
            .content {{
                padding: 30px 20px;
            }}
            
            .download-section {{
                flex-direction: column;
                text-align: center;
            }}
            
            .floating-nav {{
                display: none;
            }}
        }}
        
        @media print {{
            body {{
                background: white;
                padding: 0;
            }}
            
            .download-section,
            .floating-nav,
            .progress-indicator {{
                display: none !important;
            }}
            
            .container {{
                box-shadow: none;
                border-radius: 0;
            }}
        }}
    </style>
</head>
<body>
    <div class="progress-indicator">
        <div class="progress-bar" id="progressBar"></div>
    </div>
    
    <div class="container" id="roadmapContainer">
        <div class="header">
            <h1 class="roadmap-title">🚀 Learning Roadmap</h1>
            <p class="roadmap-subtitle">Master {', '.join(self.input_list)} in {self.time}</p>
        </div>
        
        <div class="download-section">
            <div class="download-info">
                <span>📅 Duration: <strong>{self.time}</strong></span>
                <span>🎯 Skills: <strong>{len(self.input_list)} topics</strong></span>
                <span>⏰ Generated: <strong id="currentDate"></strong></span>
            </div>
            <button class="download-btn" onclick="downloadPDF()">
                📥 Download PDF
            </button>
        </div>
        
        <div class="content" id="roadmapContent">
            {content}
        </div>
    </div>
    
    <div class="floating-nav" id="floatingNav">
        <!-- Navigation dots will be generated by JavaScript -->
    </div>
    
    <script>
        // Set current date
        document.getElementById('currentDate').textContent = new Date().toLocaleDateString();
        
        // Progress bar functionality
        function updateProgressBar() {{
            const winHeight = window.innerHeight;
            const docHeight = document.documentElement.scrollHeight - winHeight;
            const scrollTop = window.pageYOffset;
            const scrollPercent = (scrollTop / docHeight) * 100;
            document.getElementById('progressBar').style.width = scrollPercent + '%';
        }}
        
        window.addEventListener('scroll', updateProgressBar);
        
        // Floating navigation
        function createFloatingNav() {{
            const nav = document.getElementById('floatingNav');
            const titles = document.querySelectorAll('.main-title');
            
            titles.forEach((title, index) => {{
                const navItem = document.createElement('div');
                navItem.className = 'nav-item';
                navItem.title = title.textContent;
                navItem.addEventListener('click', () => {{
                    title.scrollIntoView({{ behavior: 'smooth' }});
                }});
                nav.appendChild(navItem);
            }});
            
            // Update active nav item on scroll
            window.addEventListener('scroll', () => {{
                let current = '';
                titles.forEach(title => {{
                    const rect = title.getBoundingClientRect();
                    if (rect.top <= 100) {{
                        current = title.textContent;
                    }}
                }});
                
                nav.querySelectorAll('.nav-item').forEach((item, index) => {{
                    item.classList.remove('active');
                    if (titles[index] && titles[index].textContent === current) {{
                        item.classList.add('active');
                    }}
                }});
            }});
        }}
        
        // PDF download functionality
        function downloadPDF() {{
            const button = document.querySelector('.download-btn');
            const originalText = button.innerHTML;
            button.innerHTML = '⏳ Generating...';
            button.disabled = true;
            
            const element = document.getElementById('roadmapContainer');
            const opt = {{
                margin: 0.5,
                filename: 'learning-roadmap-{"-".join(self.input_list)}.pdf',
                image: {{ type: 'jpeg', quality: 0.98 }},
                html2canvas: {{ 
                    scale: 2,
                    useCORS: true,
                    letterRendering: true
                }},
                jsPDF: {{ 
                    unit: 'in', 
                    format: 'a4', 
                    orientation: 'portrait' 
                }}
            }};
            
            html2pdf().set(opt).from(element).save().then(() => {{
                button.innerHTML = originalText;
                button.disabled = false;
            }}).catch(() => {{
                button.innerHTML = '❌ Error';
                setTimeout(() => {{
                    button.innerHTML = originalText;
                    button.disabled = false;
                }}, 2000);
            }});
        }}
        
        // Initialize
        document.addEventListener('DOMContentLoaded', () => {{
            createFloatingNav();
            updateProgressBar();
            
            // Add smooth scrolling to all internal links
            document.querySelectorAll('a[href^="#"]').forEach(anchor => {{
                anchor.addEventListener('click', function (e) {{
                    e.preventDefault();
                    const target = document.querySelector(this.getAttribute('href'));
                    if (target) {{
                        target.scrollIntoView({{ behavior: 'smooth' }});
                    }}
                }});
            }});
        }});
    </script>
</body>
</html>
        """

    def gpt_tools(self):
        print("GPT started")
        prompt = f"""
Create a STUDY ROADMAP in strict Markdown format. Use EXACT sections in this order:
# Summary
# Week-by-Week Plan
# Capstone
# Checkpoints
# Resources

Rules:
- Assume {self.time}, {self.input_list}, ~[HOURS_PER_WEEK] hrs/wk, zero background.
- # Summary: 3–5 bullets + "Day-1 Setup Checklist".
- # Week-by-Week Plan: For each week include these subheadings in order:
  ### Learning Goals
  ### Core Topics
  ### Hands-on Tasks
  ### Mini-Project
  ### Assessment
  ### Time Split (Reading X% • Coding Y% • Review Z%)
  ### Stretch Topics (Optional)
- # Capstone: Problem, Milestones, Deliverables, Rubric (5 criteria, 0–5 each).
- # Checkpoints: Week-by-week pass/fail criteria (2–4 per week).
- # Resources: 5–10 items (docs, tutorials, cheatsheets), title + 1 line why.

Constraints:
- Keep bullets concise, concrete, actionable.
- Always use same section titles/subheadings, no extras.
- Avoid emojis/tables.

        List: {', '.join(self.input_list)}
        """

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=2000
        )

        raw_markdown = response.choices[0].message.content.strip()
        processed_content = self.process_markdown_content(raw_markdown)
        return self.create_beautiful_html(processed_content)

