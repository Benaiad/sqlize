# SQLize MCP Setup Demo — Add sqlize to Claude Code

Output vhs-demo/demo-mcp.gif
Output vhs-demo/demo-mcp.mp4

Set Shell zsh
Set FontSize 15
Set Width 1400
Set Height 800
Set Padding 20
Set Theme "Catppuccin Mocha"
Set TypingSpeed 35ms
Set CursorBlink false

# 1. Add sqlize as an MCP server
Type "claude mcp add --transport stdio --env SQLIZE_SPEC_PATH=specs/github-minimal.json --env SQLIZE_BEARER_ENV_VAR=GITHUB_TOKEN --scope user sqlize-github -- sqlize mcp"
Sleep 3s
Enter
Sleep 3s

# 2. Start Claude Code
Type "claude"
Enter
Sleep 5s

# 3. Show configured MCP servers
Type "/mcp"
Enter
Sleep 4s

# 4. Exit /mcp view
Escape
Sleep 2s

# 5. Exit Claude Code
Ctrl+C
Sleep 2s
