# SQLize Demo — Query GitHub like a database

Output demo.gif
Output demo.mp4

Set Shell zsh
Set FontSize 15
Set Width 1400
Set Height 800
Set Padding 20
Set Theme "Catppuccin Mocha"
Set TypingSpeed 35ms
Set CursorBlink false

# Start sqlize REPL with full GitHub spec
Type "sqlize --spec specs/github.json"
Enter
Sleep 3s

# 1. Show available tables
Type "SHOW TABLES"
Sleep 1s
Enter
Sleep 4s

# 2. Describe a table to see columns
Type "DESCRIBE repos_issues"
Sleep 1s
Enter
Sleep 4s

# 3. Org repos — stars, forks, language
Type "SELECT name, stargazers_count, forks_count, language FROM orgs_repos WHERE org = 'openclaw' LIMIT 5;"
Sleep 2s
Enter
Sleep 5s

# 4. Top contributors by commits
Type "SELECT login, contributions FROM repos_contributors WHERE owner = 'openclaw' AND repo = 'openclaw' ORDER BY contributions DESC LIMIT 10;"
Sleep 3s
Enter
Sleep 5s

# 5. Most discussed open issues
Type "SELECT number, title, comments FROM repos_issues WHERE owner = 'openclaw' AND repo = 'openclaw' AND state = 'open' AND sort = 'comments' AND direction = 'desc' LIMIT 5;"
Sleep 4s
Enter
Sleep 5s

# 6. Open pull requests
Type "SELECT number, title, user_login FROM repos_pulls WHERE owner = 'openclaw' AND repo = 'openclaw' AND state = 'open' LIMIT 5;"
Sleep 3s
Enter
Sleep 5s

# 7. Recent releases
Type "SELECT tag_name, name, published_at FROM repos_releases WHERE owner = 'openclaw' AND repo = 'openclaw' LIMIT 5;"
Sleep 2s
Enter
Sleep 5s

# Exit
Type@0ms ""
Ctrl+D
Sleep 1s
