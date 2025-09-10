import pandas as pd
import sys
import re


def read_switch_config_from_excel(file_path: str, sheet_name: str = 'Sheet1') -> pd.DataFrame | None:
    """
    從 Excel 檔案中讀取交換器連接埠設定。

    Args:
        file_path (str): Excel 檔案的路徑。
        sheet_name (str): 要讀取的工作表名稱。

    Returns:
        pd.DataFrame | None: 包含設定資料的 DataFrame，如果發生錯誤則返回 None。
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str).fillna('')
        # 'Zone Name' 是可選欄位
        required_columns = ['switch port name', 'switch port wwpn'] 
        if not all(col in df.columns for col in required_columns):
            print(f"錯誤：Excel 檔案 '{file_path}' 必須包含以下欄位：{', '.join(required_columns)}", file=sys.stderr)
            return None
        # 統一欄位名稱以便後續處理
        df.rename(columns={'switch port name': 'Alias', 'switch port wwpn': 'WWPN', 'Zone Name': 'Zone Name'}, inplace=True)
        # 為報告增加一個空的 Port Index 欄位
        df['Port Index'] = ''
        return df
    except FileNotFoundError:
        print(f"錯誤：找不到檔案 '{file_path}'", file=sys.stderr)
        return None
    except Exception as e:
        print(f"讀取 Excel 檔案時發生錯誤：{e}", file=sys.stderr)
        return None

def parse_aliases_from_txt(file_path: str) -> dict[str, str]:
    """
    從 zoneshow 的文字輸出中解析已存在的 alias。

    Args:
        file_path (str): 包含 'zoneshow' 輸出的文字檔案路徑。

    Returns:
        dict[str, str]: 一個 WWPN 到 alias 名稱的對應字典。
    """
    wwpn_regex = re.compile(r'([0-9a-f]{2}:){7}[0-9a-f]{2}')
    alias_map = {}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            last_alias_name = None
            for line in f:
                clean_line = line.strip()
                if clean_line.startswith("alias:"):
                    # 提取 alias name
                    parts = clean_line.split(":", 1)
                    if len(parts) > 1:
                        last_alias_name = parts[1].strip()
                elif last_alias_name:
                    # 如果上一行是 alias name，這一行應該是 WWPN
                    match = wwpn_regex.search(clean_line)
                    if match:
                        alias_map[match.group(0).lower()] = last_alias_name
                        last_alias_name = None # 處理完畢，重置以尋找下一個 alias
    except Exception as e:
        print(f"警告：解析既有 alias 時發生錯誤: {e}", file=sys.stderr)
    return alias_map
    
def parse_zones_from_txt(file_path: str) -> dict[str, str]:
    """
    從 zoneshow 的文字輸出中解析已存在的 zone 和其成員 (alias)。

    Args:
        file_path (str): 包含 'zoneshow' 輸出的文字檔案路徑。

    Returns:
        dict[str, str]: 一個 alias 到 zone 名稱的對應字典。
    """
    alias_to_zone_map = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            current_zone_name = None
            in_defined_config = False
            for line in f:
                clean_line = line.strip()
                if clean_line.startswith("Defined configuration:"):
                    in_defined_config = True
                    continue
                
                if not in_defined_config or not clean_line:
                    continue

                if clean_line.startswith("zone:"):
                    parts = clean_line.split(":", 1)
                    current_zone_name = parts[1].strip() if len(parts) > 1 else None
                elif current_zone_name:
                    # 這一行是 zone 的成員
                    members = [member.strip() for member in clean_line.split(';') if member.strip()]
                    for member_alias in members:
                        alias_to_zone_map[member_alias] = current_zone_name
                    current_zone_name = None # 處理完一個 zone 的成員後立即重置
    except Exception as e:
        print(f"警告：解析既有 zone 時發生錯誤: {e}", file=sys.stderr)
    return alias_to_zone_map
    
def parse_switchshow_from_txt(file_path: str, existing_aliases: dict[str, str], existing_zones: dict[str, str]) -> pd.DataFrame | None:
    """
    從 switchshow 的文字輸出中解析 Port ID 和 WWPN。

    Args:
        file_path (str): 包含 'switchshow' 輸出的文字檔案路徑。
        existing_aliases (dict[str, str]): 已存在的 WWPN 到 alias 名稱的對應字典。
        existing_zones (dict[str, str]): 已存在的 alias 到 zone 名稱的對應字典。

    Returns:
        pd.DataFrame | None: 包含 'switch port name' 和 'switch port wwpn' 的 DataFrame，
                             如果發生錯誤或找不到資料則返回 None。
    """
    # 用於匹配 WWPN 的正規表示式 (例如: 10:00:00:00:c9:aa:bb:cc)
    wwpn_regex = re.compile(r'([0-9a-f]{2}:){7}[0-9a-f]{2}')
    port_data = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # 狀態旗標，標示是否進入 switchshow 的內容區塊
            in_switchshow_section = False
            for line in f:
                line = line.strip()
                
                # 找到 switchshow 輸出的標頭，開始解析
                if line.startswith("Index Port Address"):
                    in_switchshow_section = True
                    continue
                
                # 如果不在 switchshow 區塊，就跳過
                if not in_switchshow_section:
                    continue

                # 如果遇到下一個命令提示符，表示 switchshow 區塊結束
                if "admin>" in line:
                    in_switchshow_section = False
                    break

                # 尋找包含 WWPN 和 'F-Port' 的行
                match = wwpn_regex.search(line)
                if match and 'F-Port' in line and 'Online' in line:
                    wwpn = match.group(0).lower()
                    columns = line.split()
                    if len(columns) > 1 and columns[0].isdigit():
                        port_index = columns[0]
                        # 檢查此 WWPN 是否已有別名，若無則使用預設名稱
                        alias_name = existing_aliases.get(wwpn, f"Port_{port_index}")
                        # 檢查此 alias 是否屬於某個 zone
                        zone_name = existing_zones.get(alias_name, '') # 如果找不到則為空
                        port_data.append({'Port Index': port_index, 'Alias': alias_name, 'WWPN': wwpn, 'Zone Name': zone_name})

        return pd.DataFrame(port_data) if port_data else None
    except FileNotFoundError:
        print(f"錯誤：找不到檔案 '{file_path}'", file=sys.stderr)
        return None
    except Exception as e:
        print(f"讀取或解析 TXT 檔案時發生錯誤：{e}", file=sys.stderr)
        return None

def generate_brocade_alias_commands(config_df: pd.DataFrame) -> list[str]:
    """
    從 DataFrame 產生 Brocade 'alicreate' 指令。

    Args:
        config_df (pd.DataFrame): 包含 'Alias' 和 'WWPN' 的 DataFrame。

    Returns:
        list[str]: 一個包含 'alicreate' 指令字串的列表。
    """
    commands = []
    # 迭代 DataFrame 的每一行
    for index, row in config_df.iterrows():
        alias_name = row['Alias'].strip()
        wwpn = row['WWPN'].strip()

        # 確保 alias_name 和 wwpn 都不為空
        if alias_name and wwpn:
            # 組成 Brocade alicreate 指令
            # 格式: alicreate "alias_name", "wwpn1;wwpn2;..."
            command = f'alicreate "{alias_name}", "{wwpn}"'
            commands.append(command)
        else:
            print(f"警告：第 {index + 2} 行的資料不完整，已跳過。Name: '{alias_name}', WWPN: '{wwpn}'", file=sys.stderr)
            
    return commands

def generate_brocade_zone_commands(config_df: pd.DataFrame) -> list[str]:
    """
    從 DataFrame 產生 Brocade 'zonecreate' 指令。

    Args:
        config_df (pd.DataFrame): 包含 'Alias' 和 'Zone Name' 的 DataFrame。

    Returns:
        list[str]: 一個包含 'zonecreate' 指令字串的列表。
    """
    commands = []
    # 按 'Zone Name' 分組，並過濾掉沒有 Zone Name 的資料
    zoned_df = config_df[config_df['Zone Name'].notna() & (config_df['Zone Name'] != '')]
    
    if zoned_df.empty:
        return commands

    for zone_name, group in zoned_df.groupby('Zone Name'):
        # 取得該 zone 的所有 alias 成員
        members = group['Alias'].tolist()
        members_str = ";".join(members)
        command = f'zonecreate "{zone_name}", "{members_str}"'
        commands.append(command)
        
    return commands

def export_to_excel(report_df: pd.DataFrame, file_path: str):
    """
    將 DataFrame 匯出成 Excel 報告。

    Args:
        report_df (pd.DataFrame): 要匯出的資料。
        file_path (str): 匯出的 Excel 檔案路徑。
    """
    try:
        # 確保報告的欄位順序是固定的
        report_columns = ['Port Index', 'Alias', 'WWPN', 'Zone Name']
        # 過濾掉不存在的欄位，以防萬一
        report_df_ordered = report_df[[col for col in report_columns if col in report_df.columns]]
        report_df_ordered.to_excel(file_path, index=False, engine='openpyxl')
        print(f"報告已成功儲存至 '{file_path}'")
    except Exception as e:
        print(f"錯誤：無法匯出 Excel 報告 '{file_path}': {e}", file=sys.stderr)

def main():
    """
    主執行函數。
    """
    # --- 設定來源檔案 ---
    # 選擇一：從 Excel 檔案讀取
    # source_mode = "excel"
    # source_file = 'san_config.xlsx'

    # 選擇二：從 switchshow 的 txt 檔案讀取
    source_mode = "txt"
    source_file = 'bq_3F_switch_info.txt'
    # --------------------

    report_file = 'san_port_report.xlsx'
    output_file = 'switch_commands.txt'

    # 1. 根據模式讀取設定
    config_data = None
    if source_mode == "excel":
        config_data = read_switch_config_from_excel(source_file)
    elif source_mode == "txt":
        # 1a. 從 txt 檔案中解析出已存在的 alias
        existing_aliases = parse_aliases_from_txt(source_file)
        # 1b. 從 txt 檔案中解析出已存在的 zone
        existing_zones = parse_zones_from_txt(source_file)
        # 1c. 解析 switchshow，並傳入已存在的 alias 和 zone 資訊
        config_data = parse_switchshow_from_txt(source_file, existing_aliases, existing_zones)
    else:
        print(f"錯誤：不支援的來源模式 '{source_mode}'", file=sys.stderr)
        config_data = None

    if config_data is not None:
        # 2. 產生 Alias 指令
        alias_commands = generate_brocade_alias_commands(config_data)
        # 3. 產生 Zone 指令
        zone_commands = generate_brocade_zone_commands(config_data)

        # 合併所有指令
        switch_commands = alias_commands + zone_commands

        if switch_commands:
            # 4. 在螢幕上印出指令
            print("--- 產生的交換器指令 ---")
            for cmd in switch_commands:
                print(cmd)
            print("--------------------------")

            # 4. 將指令儲存到檔案
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    for cmd in switch_commands:
                        f.write(cmd + '\n')
                print(f"\n指令已成功儲存至 '{output_file}'")
            except IOError as e:
                print(f"錯誤：無法寫入檔案 '{output_file}': {e}", file=sys.stderr)
        else:
            print("沒有產生任何指令。請檢查您的 Excel 檔案內容。")
        
        # 6. 無論是否有指令，只要有資料就匯出報告
        export_to_excel(config_data, report_file)

if __name__ == "__main__":
    main()
