
@echo off
:: Ejecución proyecto CXS
echo Activando el entorno virtual...
echo ===============================
call venv\Scripts\activate

echo Ejecutando la automatizacion...
python Scripts/main.py


pause


