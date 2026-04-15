#define MyAppName "NanoMQ TAOS"
#define MyAppVersion "0.24.15"
#define MyAppPublisher "EMQ Edge Computing Team"
#define MyAppExeName "nanomq.exe"
#define MyAppCliExeName "nanomq_cli.exe"
#define MyAppNngCatExeName "nngcat.exe"
#define MyAutostartTaskName "NanoMQ TAOS Autostart"
#define MySourceDir "dist-win64-taos-bundled\\nanomq-nng-v0.24.15-taos"

[Setup]
AppId={{7F6C0F3A-918C-4A97-A1E8-CF92AF0A3557}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppVerName={#MyAppName} {#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL=https://nanomq.io/
AppSupportURL=https://github.com/emqx/nanomq/discussions
AppUpdatesURL=https://github.com/emqx/nanomq/releases
DefaultDirName={autopf}\NanoMQ TAOS
DefaultGroupName=NanoMQ TAOS
UninstallDisplayIcon={app}\bin\{#MyAppExeName}
AllowNoIcons=yes
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64
ChangesEnvironment=yes
CloseApplications=yes
Compression=lzma
SolidCompression=yes
WizardStyle=modern
LicenseFile=LICENSE.txt
OutputDir=dist-win64-taos-bundled\installer
OutputBaseFilename=nanomq-taos-win64-{#MyAppVersion}
PrivilegesRequired=admin
SetupLogging=yes

[Types]
Name: "full"; Description: "Full installation"
Name: "runtime"; Description: "Runtime installation"
Name: "custom"; Description: "Custom installation"; Flags: iscustom

[Components]
Name: "runtime"; Description: "NanoMQ broker runtime"; Types: full runtime custom; Flags: fixed
Name: "tools"; Description: "Command-line tools"; Types: full custom
Name: "config"; Description: "Configuration and TDengine schema files"; Types: full runtime custom
Name: "devel"; Description: "Headers, libraries, and CMake package files"; Types: full custom

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"
Name: "chinesesimplified"; MessagesFile: "compiler:Languages\ChineseSimplified.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; Flags: unchecked
Name: "quickstart"; Description: "Create broker start/stop shortcuts"; Flags: checkedonce
Name: "autostart"; Description: "Start NanoMQ TAOS automatically at system startup"; Flags: unchecked

[Dirs]
Name: "{app}\bin"; Components: runtime
Name: "{app}\config"; Components: config
Name: "{app}\include"; Components: devel
Name: "{app}\lib"; Components: devel
Name: "{app}\share"; Components: devel
Name: "{app}\log"; Components: runtime
Name: "{app}\data"; Components: runtime

[Files]
Source: "{#MySourceDir}\bin\{#MyAppExeName}"; DestDir: "{app}\bin"; Components: runtime; Flags: ignoreversion
Source: "{#MySourceDir}\bin\libwinpthread-1.dll"; DestDir: "{app}\bin"; Components: runtime; Flags: ignoreversion
Source: "{#MySourceDir}\bin\{#MyAppCliExeName}"; DestDir: "{app}\bin"; Components: tools; Flags: ignoreversion
Source: "{#MySourceDir}\bin\{#MyAppNngCatExeName}"; DestDir: "{app}\bin"; Components: tools; Flags: ignoreversion
Source: "{#MySourceDir}\config\*"; DestDir: "{app}\config"; Components: config; Flags: ignoreversion recursesubdirs createallsubdirs onlyifdoesntexist
Source: "{#MySourceDir}\include\*"; DestDir: "{app}\include"; Components: devel; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#MySourceDir}\lib\*"; DestDir: "{app}\lib"; Components: devel; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#MySourceDir}\share\*"; DestDir: "{app}\share"; Components: devel; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "LICENSE.txt"; DestDir: "{app}"; Flags: ignoreversion

[Registry]
Root: HKLM; Subkey: "SYSTEM\CurrentControlSet\Control\Session Manager\Environment"; ValueType: expandsz; ValueName: "NANOMQ"; ValueData: "{app}"; Flags: preservestringtype uninsdeletevalue
Root: HKLM; Subkey: "SYSTEM\CurrentControlSet\Control\Session Manager\Environment"; ValueType: expandsz; ValueName: "NANOSDK_LIB_DIR"; ValueData: "{app}\lib"; Flags: preservestringtype uninsdeletevalue

[Icons]
Name: "{group}\NanoMQ TAOS"; Filename: "{app}"
Name: "{group}\Start NanoMQ (Default Config)"; Filename: "{app}\bin\{#MyAppExeName}"; Parameters: "start --conf ""{app}\config\nanomq.conf"""; WorkingDir: "{app}"; Tasks: quickstart
Name: "{group}\Start NanoMQ (Weld TAOS Config)"; Filename: "{app}\bin\{#MyAppExeName}"; Parameters: "start --conf ""{app}\config\nanomq_weld_taos.conf"""; WorkingDir: "{app}"; Tasks: quickstart
Name: "{group}\Stop NanoMQ"; Filename: "{app}\bin\{#MyAppExeName}"; Parameters: "stop"; WorkingDir: "{app}"; Tasks: quickstart
Name: "{group}\NanoMQ CLI"; Filename: "{app}\bin\{#MyAppCliExeName}"; WorkingDir: "{app}"; Components: tools
Name: "{group}\nngcat"; Filename: "{app}\bin\{#MyAppNngCatExeName}"; WorkingDir: "{app}"; Components: tools
Name: "{group}\Config Folder"; Filename: "{app}\config"
Name: "{group}\Install Folder"; Filename: "{app}"
Name: "{group}\Uninstall NanoMQ TAOS"; Filename: "{uninstallexe}"
Name: "{autodesktop}\NanoMQ TAOS"; Filename: "{app}"; Tasks: desktopicon

[Run]
Filename: "{app}\config"; Description: "Open installed config folder"; Flags: postinstall shellexec skipifsilent unchecked

[UninstallDelete]
Type: filesandordirs; Name: "{app}\log"
Type: filesandordirs; Name: "{app}\data"

[Code]
function GetAutostartScriptPath: string;
begin
  Result := ExpandConstant('{app}\start-nanomq-taos.cmd');
end;

function GetAutostartTaskName: string;
begin
  Result := '{#MyAutostartTaskName}';
end;

function ExecHiddenAndWait(const FileName, Params: string; var ResultCode: Integer): Boolean;
begin
  Log(Format('Exec: %s %s', [FileName, Params]));
  Result := Exec(FileName, Params, '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  Log(Format('Exec result: success=%d code=%d', [Ord(Result), ResultCode]));
end;

function ScheduledTaskExists(const TaskName: string): Boolean;
var
  ResultCode: Integer;
begin
  Result :=
    ExecHiddenAndWait(
      ExpandConstant('{sys}\schtasks.exe'),
      '/Query /TN "' + TaskName + '"',
      ResultCode) and
    (ResultCode = 0);
end;

function DeleteScheduledTask(const TaskName: string): Boolean;
var
  ResultCode: Integer;
begin
  if not ScheduledTaskExists(TaskName) then begin
    Result := True;
    Exit;
  end;

  Result :=
    ExecHiddenAndWait(
      ExpandConstant('{sys}\schtasks.exe'),
      '/Delete /TN "' + TaskName + '" /F',
      ResultCode) and
    (ResultCode = 0);
end;

function WriteAutostartScript: Boolean;
var
  Script: string;
begin
  Script :=
    '@echo off' + #13#10 +
    'cd /d "%~dp0"' + #13#10 +
    '"%~dp0bin\{#MyAppExeName}" start --conf "%~dp0config\nanomq_weld_taos.conf"' + #13#10;
  Result := SaveStringToFile(GetAutostartScriptPath, Script, False);
end;

function CreateScheduledAutostartTask: Boolean;
var
  ResultCode: Integer;
  Params: string;
begin
  if not WriteAutostartScript then begin
    Log('Failed to write autostart helper script.');
    Result := False;
    Exit;
  end;

  Params :=
    '/Create /TN "' + GetAutostartTaskName + '"' +
    ' /TR "\"' + GetAutostartScriptPath + '\""' +
    ' /SC ONSTART /RU SYSTEM /RL HIGHEST /F';

  Result :=
    ExecHiddenAndWait(
      ExpandConstant('{sys}\schtasks.exe'),
      Params,
      ResultCode) and
    (ResultCode = 0);
end;

procedure UpdateAutostartTask;
begin
  if not DeleteScheduledTask(GetAutostartTaskName) then
    Log('Warning: failed to delete existing autostart task before update.');

  if WizardIsTaskSelected('autostart') then begin
    if not CreateScheduledAutostartTask then begin
      MsgBox(
        'NanoMQ was installed, but the startup task could not be created.' + #13#10 +
        'You can rerun the installer or create the task manually later.',
        mbError, MB_OK);
    end;
  end;
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then begin
    UpdateAutostartTask;
  end;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if CurUninstallStep = usUninstall then begin
    if not DeleteScheduledTask(GetAutostartTaskName) then
      Log('Warning: failed to delete autostart task during uninstall.');
  end;
end;
