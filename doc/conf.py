author = 'Weizheng Lu'
bibtex_bibfiles = ['references.bib']
bibtex_reference_style = 'author_year'
comments_config = {'hypothesis': False, 'utterances': False}
copyright = '2023-2024'
exclude_patterns = ['**.ipynb_checkpoints', '.DS_Store', 'Thumbs.db', '_build']
extensions = ['sphinx_togglebutton', 'sphinx_copybutton', 'myst_nb', 'jupyter_book', 'sphinx_thebe', 'sphinx_comments', 'sphinx_external_toc', 'sphinx.ext.intersphinx', 'sphinx_design', 'sphinx_book_theme', 'sphinxcontrib.bibtex', 'sphinx_jupyterbook_latex']
external_toc_exclude_missing = True
external_toc_path = '_toc.yml'
html_baseurl = ''
html_favicon = "_static/logo.ico"
html_logo = 'logo.svg'
html_sourcelink_suffix = ''
html_theme = 'sphinx_book_theme'
html_theme_options = {
    'search_bar_text': '搜索...', 
    'launch_buttons': {
        'notebook_interface': 'classic', 
        'binderhub_url': '', 
        'jupyterhub_url': '', 
        'thebe': False, 
        'colab_url': 'https://colab.research.google.com'
    },
    'path_to_docs': 'docs', 
    'repository_url': 'https://github.com/godaai/flink-book-zh', 
    'repository_branch': 'main', 
    'extra_footer': '', 
    'home_page_in_toc': True,
    'icon_links': [
        {
            "name": "GitHub",
            "url": "https://github.com/godaai/flink-book-zh",
            "icon": "https://img.shields.io/github/stars/godaai/flink-book-zh?style=for-the-badge",
            "type": "url",
        },
    ], 
    'announcement': "如果你觉得内容对你有帮助，请在 <a href=\"https://github.com/godaai/flink-book-zh\">GitHub</a> 上点个 star 吧！", 
    'analytics': {'google_analytics_id': ''}, 
    'use_repository_button': True, 
    'use_edit_page_button': False, 
    'use_issues_button': False,
    "toc_title": "本节目录",
}
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_js_files = [
    "https://cdnjs.cloudflare.com/ajax/libs/require.js/2.3.6/require.min.js",
]
html_title = 'Flink 原理与实践'
latex_engine = 'pdflatex'
myst_enable_extensions = ['colon_fence', 'dollarmath', 'linkify', 'substitution', 'tasklist']
myst_url_schemes = ['mailto', 'http', 'https']
nb_execution_allow_errors = False
nb_execution_cache_path = ''
nb_execution_excludepatterns = []
nb_execution_in_temp = False
nb_execution_mode = 'off'
nb_execution_timeout = 30
nb_output_stderr = 'show'
numfig = True
numfig_format = {'figure': '图 %s', 'table': '表 %s', 'code-block': '代码清单 %s', 'section': '章节 %s'}
pygments_style = 'sphinx'
suppress_warnings = ['myst.domains']
use_jupyterbook_latex = True
use_multitoc_numbering = True
