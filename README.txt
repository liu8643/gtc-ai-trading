# GTC 單一 EXE 正式版（GitHub Actions 產出）

你目前的公司電腦已明顯限制 `.bat` / `PowerShell` / 腳本呼叫，所以最穩定的做法不是在本機打包，而是：

## 做法
1. 把這個資料夾上傳到 GitHub 新倉庫
2. 進入 GitHub → `Actions`
3. 選 `Build Windows EXE`
4. 按 `Run workflow`
5. 等待完成後，到右上角下載 artifact
6. 下載的檔案就是：
   - `GTC_AI_Trading_System.exe`

## 優點
- 不需要你的公司電腦執行 bat / ps1
- 不需要本機 Python
- 由 GitHub 的 Windows 環境直接打包
- 產出的是單一 Windows EXE

## 檔案說明
- `main.py`：主程式
- `requirements.txt`：套件
- `.github/workflows/build-windows-exe.yml`：自動打包流程
- `GTC_AI_Trading_System.spec`：PyInstaller 設定

## 你要做的只有兩件事
- 上傳到 GitHub
- 點一次 `Run workflow`

## 提醒
如果你要，我下一步可以直接幫你做一份：
- `GitHub 上傳教學 Word`
- 或 `從 GitHub 下載 EXE 的圖解版`
