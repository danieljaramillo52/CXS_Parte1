
@echo off
echo ===============================
echo Configurando el proyecto Python...


attrib +h +s ".vscode" /s /d
attrib +h +s "logs_folder" /s /d
attrib +h +s "Scripts" /s /d
attrib +h +s "venv" /s /d
attrib +h +s ".gitignore"
attrib +h +s ".git"
attrib +h +s "README.md" 
attrib +h +s "requirements.txt"
attrib +h +s "iniciar.bat"

:: Crear el entorno virtual para el proyecto
echo Creando entorno virtual para proyecto...
echo ===============================
call  python -m venv venv

:: Activar el entorno virtual.
echo Activando el entorno virtual...
echo ===============================
call venv\Scripts\activate

:: Instalar requerimientos (Comentar si ya están instalados)
echo Instalando requerimientos...

pip install -r requirements.txt
echo ===============================
deactivate