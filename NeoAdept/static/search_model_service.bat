@echo off

setlocal

set SEARCH_DOC_FOLDER=D:\repos\git\NeoAdept_ATS\Resume
set "INDEX_FOLDER=%~dp0index_folder"
set "CV_DOC_FOLDER=%~dp0cv_files"
set "PROFILE_PIC_FOLDER=%~dp0profile_pic_files"
call D:\repos\git\NeoAdept_ATS\venv\Scripts\activate.bat

python "%~dp0\search_module_service.py"

endlocal
