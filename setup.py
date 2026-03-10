#!/usr/bin/env python3
"""
SQL Analyzer Setup Script
Installs the SQL analyzer and git hook in your repository
"""

import os
import sys
import subprocess
import platform
import shutil
from pathlib import Path

# Fix Windows console encoding for emoji support
if platform.system() == 'Windows':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


def print_header(message):
    """Print a formatted header message"""
    print("\n" + "=" * 70)
    print(f"  {message}")
    print("=" * 70 + "\n")


def print_step(emoji, message):
    """Print a step message with emoji"""
    print(f"{emoji} {message}")


def print_success(message):
    """Print a success message"""
    print(f"✅ {message}")


def print_error(message):
    """Print an error message"""
    print(f"❌ {message}")


def print_warning(message):
    """Print a warning message"""
    print(f"⚠️  {message}")


def check_python_version():
    """Check if Python version is 3.12 or higher"""
    print_step("🐍", "Checking Python version...")
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 12):
        print_error(f"Python 3.12+ required, found {version.major}.{version.minor}.{version.micro}")
        return False
    print_success(f"Python {version.major}.{version.minor}.{version.micro} detected")
    return True


def check_git_installed():
    """Check if git is installed"""
    print_step("📦", "Checking for git installation...")
    try:
        result = subprocess.run(['git', '--version'],
                              capture_output=True,
                              text=True,
                              check=True)
        print_success(f"Git detected: {result.stdout.strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print_error("Git is not installed or not in PATH")
        return False


def check_claude_installed():
    """Check if Claude Code CLI is installed"""
    print_step("🤖", "Checking for Claude Code CLI...")
    try:
        result = subprocess.run(['claude', '--version'],
                              capture_output=True,
                              text=True,
                              check=True)
        print_success(f"Claude Code detected: {result.stdout.strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print_warning("Claude Code CLI not found in current PATH")
        print_warning("The git hook includes PATH fixes for npm-installed claude")
        print_warning("If you have claude installed, you can continue")
        response = input("  Continue anyway? (y/n): ").strip().lower()
        return response == 'y'


def check_git_repository():
    """Check if current directory is a git repository"""
    print_step("📂", "Checking if in a git repository...")
    try:
        subprocess.run(['git', 'rev-parse', '--git-dir'],
                      capture_output=True,
                      check=True)
        print_success("Git repository detected")
        return True
    except subprocess.CalledProcessError:
        print_error("Not in a git repository")
        print_warning("Run 'git init' to create a repository first")
        return False


def create_folder_structure():
    """Create required folder structure"""
    print_step("📁", "Creating folder structure...")

    folders = [
        'queries',
        'generated/validation',
        'generated/documentation',
        'generated/improvements'
    ]

    created = []
    exists = []

    for folder in folders:
        path = Path(folder)
        if path.exists():
            exists.append(folder)
        else:
            path.mkdir(parents=True, exist_ok=True)
            created.append(folder)

    if created:
        print_success(f"Created folders: {', '.join(created)}")
    if exists:
        print_warning(f"Already exist: {', '.join(exists)}")

    return True


def copy_analyzer_script():
    """Copy analyze_sql.py to current directory"""
    print_step("📄", "Checking for analyze_sql.py...")

    analyzer_path = Path('analyze_sql.py')

    if analyzer_path.exists():
        print_success("analyze_sql.py found in current directory")
        return True
    else:
        print_error("analyze_sql.py not found in current directory")
        print_warning("Please ensure analyze_sql.py is in the same directory as setup.py")
        return False


def install_git_hook():
    """Install the post-commit git hook"""
    print_step("🪝", "Installing git post-commit hook...")

    hook_path = Path('.git/hooks/post-commit')

    # Get the user's home directory for PATH fix
    home_dir = str(Path.home()).replace('\\', '/')

    # Create the hook content
    hook_content = f'''#!/bin/sh
# Set PATH to include common locations for claude
export PATH="$PATH:/c/Users/{os.getlogin()}/AppData/Roaming/npm:$HOME/AppData/Roaming/npm"

# Git post-commit hook to analyze SQL files

echo "🔍 Checking for SQL files in this commit..."

# Get list of SQL files changed in the last commit
SQL_FILES=$(git diff-tree --no-commit-id --name-only -r HEAD | grep '\\.sql$' | grep '^queries/')

if [ -z "$SQL_FILES" ]; then
    echo "ℹ️  No SQL files in queries/ folder to analyze"
    exit 0
fi

echo "📊 Found SQL files to analyze:"
echo "$SQL_FILES"

# Analyze each SQL file
for file in $SQL_FILES; do
    if [ -f "$file" ]; then
        echo ""
        echo "🤖 Analyzing: $file"
        python analyze_sql.py "$file"

        if [ $? -eq 0 ]; then
            echo "✅ Analysis complete for $file"
        else
            echo "⚠️  Analysis failed for $file (continuing anyway)"
        fi
    fi
done

echo ""
echo "🎉 All SQL files analyzed! Check the generated/ folder for artifacts."
'''

    try:
        # Create hooks directory if it doesn't exist
        hook_path.parent.mkdir(parents=True, exist_ok=True)

        # Write the hook
        with open(hook_path, 'w', newline='\n') as f:
            f.write(hook_content)

        # Set executable permissions on Unix-like systems
        if platform.system() != 'Windows':
            os.chmod(hook_path, 0o755)
            print_success("Git hook installed with executable permissions")
        else:
            print_success("Git hook installed")

        return True
    except Exception as e:
        print_error(f"Failed to install git hook: {e}")
        return False


def test_installation():
    """Test that everything is installed correctly"""
    print_step("🧪", "Testing installation...")

    checks = {
        'queries/ folder': Path('queries').exists(),
        'generated/validation/ folder': Path('generated/validation').exists(),
        'generated/documentation/ folder': Path('generated/documentation').exists(),
        'generated/improvements/ folder': Path('generated/improvements').exists(),
        'analyze_sql.py script': Path('analyze_sql.py').exists(),
        'post-commit hook': Path('.git/hooks/post-commit').exists()
    }

    all_passed = True
    for check, passed in checks.items():
        if passed:
            print_success(f"{check}")
        else:
            print_error(f"{check}")
            all_passed = False

    return all_passed


def print_next_steps():
    """Print next steps for the user"""
    print_header("🎉 Installation Complete!")

    print("""
Your SQL Analyzer is now set up and ready to use!

📝 Next Steps:

1. Add SQL files to the queries/ folder:
   cp your_query.sql queries/

2. Commit the SQL file:
   git add queries/your_query.sql
   git commit -m "Add new SQL query"

3. The hook will automatically:
   ✨ Detect SQL files in queries/ folder
   ✨ Generate documentation (README.md)
   ✨ Generate improvement suggestions
   ✨ Generate validation queries
   ✨ Save artifacts to generated/ folder

4. Review and commit the generated artifacts:
   git add generated/
   git commit -m "Add generated artifacts"
   git push

⏱️  Note: Analysis takes 4-8 minutes per SQL file

📚 Generated Artifacts:

   generated/documentation/    - Comprehensive SQL documentation
   generated/improvements/     - Performance & quality suggestions
   generated/validation/       - Test queries for validation

🔧 Troubleshooting:

   - If claude CLI not found: Check PATH includes npm global bin
   - If analysis fails: Ensure claude CLI is working (run: claude --version)
   - For help: Check the project README or contact support

Happy analyzing! 🚀
""")


def main():
    """Main setup function"""
    print_header("SQL Analyzer Setup")
    print("This script will install the SQL analyzer and git hook in your repository.\n")

    # Run prerequisite checks
    checks = [
        ("Python 3.12+", check_python_version),
        ("Git", check_git_installed),
        ("Claude Code CLI", check_claude_installed),
        ("Git Repository", check_git_repository),
    ]

    print_header("🔍 Checking Prerequisites")

    all_checks_passed = True
    for name, check_func in checks:
        if not check_func():
            all_checks_passed = False

    if not all_checks_passed:
        print("\n" + "=" * 70)
        print_error("Prerequisites not met. Please fix the issues above and try again.")
        sys.exit(1)

    # Perform installation steps
    print_header("🚀 Installing SQL Analyzer")

    steps = [
        create_folder_structure,
        copy_analyzer_script,
        install_git_hook,
    ]

    for step in steps:
        if not step():
            print_error("Installation failed. Please fix the issues and try again.")
            sys.exit(1)

    # Test installation
    print_header("✅ Verifying Installation")

    if not test_installation():
        print_error("Some checks failed. Please review and fix issues.")
        sys.exit(1)

    # Print success and next steps
    print_next_steps()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Installation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        sys.exit(1)
