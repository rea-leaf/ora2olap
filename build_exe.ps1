$ErrorActionPreference = "Stop"
$OutputExeName = "Oracle表结构转Doris_ClickHouse工具.exe"

Write-Host "[1/4] 安装依赖..."
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

Write-Host "[2/4] 清理旧构建..."
if (Test-Path ".\build") { Remove-Item ".\build" -Recurse -Force }
if (Test-Path ".\dist") { Remove-Item ".\dist" -Recurse -Force }
if (Test-Path ".\oracle2ch_gui.spec") { Remove-Item ".\oracle2ch_gui.spec" -Force }

Write-Host "[3/4] 开始打包..."
python -m PyInstaller `
  --noconfirm `
  --onefile `
  --noconsole `
  --name Oracle2CH `
  oracle2ch_gui.py

Move-Item ".\dist\Oracle2CH.exe" ".\dist\$OutputExeName" -Force
Write-Host "[4/4] 打包完成，输出文件: .\dist\$OutputExeName"
