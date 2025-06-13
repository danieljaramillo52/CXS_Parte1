
:: Ejecución proyecto CXS
echo Activando el entorno virtual...
echo ===============================
call venv\Scripts\activate

echo Ejecutando la automatización...
cd Scripts
python main.py

echo Desactivando entorno virtual...
deactivate

