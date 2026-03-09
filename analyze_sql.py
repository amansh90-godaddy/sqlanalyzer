#!/usr/bin/env python3
"""
SQL Analyzer Script - Automated SQL Analysis using Claude Code + Atlassian MCP

This script analyzes SQL queries and generates:
1. Validation SQL queries for data quality checks
2. README documentation explaining business logic
3. Improvement suggestions for optimization

Usage:
    python analyze_sql.py queries/wam_site_performance.sql
"""

import os
import sys
import re
import subprocess
import argparse
import shutil
from pathlib import Path
from typing import Optional, Tuple

# Fix Windows console encoding for emoji support
if sys.platform == "win32":
    try:
        # Set UTF-8 encoding for stdout/stderr on Windows
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        # Python < 3.7 fallback
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')


class SQLAnalyzer:
    """Analyzes SQL queries using Claude Code and Atlassian MCP integration."""

    def __init__(self, sql_file_path: str):
        """
        Initialize the SQL analyzer.

        Args:
            sql_file_path: Path to the SQL file to analyze
        """
        self.sql_file_path = Path(sql_file_path)
        self.sql_content = ""
        self.jira_ticket = None
        self.jira_context = ""
        self.base_name = self.sql_file_path.stem  # filename without extension

        # Find Claude CLI executable (handles Windows .cmd extension)
        self.claude_cli = shutil.which('claude')
        if not self.claude_cli:
            raise RuntimeError("Claude CLI not found in PATH. Please install Claude Code CLI.")

        # Output directories
        self.validation_dir = Path("generated/validation")
        self.documentation_dir = Path("generated/documentation")
        self.improvements_dir = Path("generated/improvements")

    def validate_file(self) -> bool:
        """
        Validate that the SQL file exists and is readable.

        Returns:
            True if file is valid, False otherwise
        """
        if not self.sql_file_path.exists():
            print(f"❌ Error: File not found: {self.sql_file_path}")
            return False

        if not self.sql_file_path.is_file():
            print(f"❌ Error: Not a file: {self.sql_file_path}")
            return False

        if self.sql_file_path.suffix.lower() not in ['.sql', '.txt']:
            print(f"⚠️  Warning: File does not have .sql extension: {self.sql_file_path}")

        return True

    def read_sql_file(self) -> bool:
        """
        Read the SQL file content.

        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"📖 Reading SQL file: {self.sql_file_path}")
            with open(self.sql_file_path, 'r', encoding='utf-8') as f:
                self.sql_content = f.read()
            print(f"✅ Successfully read {len(self.sql_content)} characters")
            return True
        except Exception as e:
            print(f"❌ Error reading file: {e}")
            return False

    def extract_jira_ticket(self) -> Optional[str]:
        """
        Extract JIRA ticket ID from SQL comments.

        Looks for patterns like:
        -- JIRA: HAT-3917
        -- Jira: PROJECT-123

        Returns:
            JIRA ticket ID if found, None otherwise
        """
        print("🔍 Searching for JIRA ticket reference...")

        # Pattern to match JIRA ticket IDs in comments
        patterns = [
            r'--\s*JIRA:\s*([A-Z]+-\d+)',  # -- JIRA: HAT-3917
            r'--\s*Jira:\s*([A-Z]+-\d+)',  # -- Jira: HAT-3917
            r'/\*\s*JIRA:\s*([A-Z]+-\d+)',  # /* JIRA: HAT-3917 */
        ]

        for pattern in patterns:
            match = re.search(pattern, self.sql_content, re.IGNORECASE)
            if match:
                self.jira_ticket = match.group(1)
                print(f"✅ Found JIRA ticket: {self.jira_ticket}")
                return self.jira_ticket

        print("⚠️  No JIRA ticket found in SQL comments")
        return None

    def fetch_jira_context(self) -> bool:
        """
        Fetch JIRA ticket context using Atlassian MCP.

        Returns:
            True if successful (or skipped), False on error
        """
        if not self.jira_ticket:
            print("ℹ️  Skipping JIRA context fetch (no ticket ID found)")
            return True

        print(f"🔗 Fetching JIRA context for {self.jira_ticket} using Atlassian MCP...")

        try:
            # Use Claude Code to fetch JIRA context via Atlassian MCP
            prompt = f"Using the Atlassian MCP server, search for and summarize JIRA ticket {self.jira_ticket}. Include: title, description, acceptance criteria, and any relevant technical details. If you cannot access the ticket, respond with 'JIRA_NOT_FOUND'."

            # Unset CLAUDECODE env var to allow nested execution
            env = os.environ.copy()
            env.pop('CLAUDECODE', None)

            result = subprocess.run(
                [self.claude_cli, 'code', '--print', '-'],
                input=prompt,
                capture_output=True,
                text=True,
                timeout=30,
                env=env
            )

            if result.returncode == 0:
                # Debug: Show what Claude Code returned
                print(f"🔍 MCP Response preview: {result.stdout[:500] if result.stdout else '(empty)'}")

                self.jira_context = result.stdout.strip() if result.stdout else ""

                # Check if we got actual JIRA context
                if not self.jira_context:
                    print(f"⚠️  JIRA context is empty - MCP may not be responding")
                    self.jira_context = f"JIRA ticket {self.jira_ticket} referenced (MCP returned empty response)"
                elif 'JIRA_NOT_FOUND' in self.jira_context:
                    print(f"⚠️  Could not access JIRA ticket {self.jira_ticket} - Ticket not found or no access")
                    self.jira_context = f"JIRA ticket {self.jira_ticket} referenced but context not available"
                elif len(self.jira_context) < 50:
                    print(f"⚠️  JIRA context suspiciously short ({len(self.jira_context)} chars) - may not have fetched correctly")
                    print(f"    Full response: {self.jira_context}")
                else:
                    print(f"✅ Successfully fetched JIRA context ({len(self.jira_context)} chars)")
                    print(f"    Preview: {self.jira_context[:200]}...")

                return True
            else:
                print(f"⚠️  Warning: Could not fetch JIRA context (exit code {result.returncode})")
                if result.stderr:
                    print(f"    stderr: {result.stderr[:300]}")
                if result.stdout:
                    print(f"    stdout: {result.stdout[:300]}")
                self.jira_context = f"JIRA ticket {self.jira_ticket} referenced (context unavailable)"
                return True  # Continue anyway

        except subprocess.TimeoutExpired:
            print("⚠️  Warning: JIRA context fetch timed out")
            self.jira_context = f"JIRA ticket {self.jira_ticket} referenced (timeout)"
            return True  # Continue anyway

        except Exception as e:
            print(f"⚠️  Warning: Error fetching JIRA context: {e}")
            self.jira_context = f"JIRA ticket {self.jira_ticket} referenced (error)"
            return True  # Continue anyway

    def ensure_output_directories(self):
        """Create output directories if they don't exist."""
        print("📁 Ensuring output directories exist...")
        self.validation_dir.mkdir(parents=True, exist_ok=True)
        self.documentation_dir.mkdir(parents=True, exist_ok=True)
        self.improvements_dir.mkdir(parents=True, exist_ok=True)
        print("✅ Output directories ready")

    def generate_validation_query(self) -> bool:
        """
        Generate validation SQL query using Claude Code.

        Returns:
            True if successful, False otherwise
        """
        print("\n🔬 Generating validation SQL query...")

        try:
            output_file = self.validation_dir / f"{self.base_name}_validation.sql"

            # Build comprehensive prompt
            prompt = f"""Analyze this SQL query and generate a comprehensive validation SQL script.

SQL FILE: {self.sql_file_path.name}
{f"JIRA CONTEXT: {self.jira_context}" if self.jira_context else ""}

SQL QUERY:
```sql
{self.sql_content}
```

Generate a validation SQL script that includes:
1. Row count checks for source tables
2. Null value checks for critical columns
3. Duplicate detection queries
4. Join validation (check for orphaned records)
5. Business rule validation (e.g., date ranges, value constraints)
6. Data type validation
7. Aggregate metric verification

Format as executable SQL with clear comments. Include expected results as comments.
Output ONLY the SQL validation script, no explanatory text before or after."""

            # Call Claude Code (pass prompt via stdin to avoid command-line length limits)
            # Unset CLAUDECODE env var to allow nested execution
            env = os.environ.copy()
            env.pop('CLAUDECODE', None)

            result = subprocess.run(
                [self.claude_cli, 'code', '--print', '-'],
                input=prompt,
                capture_output=True,
                text=True,
                timeout=120,
                env=env
            )

            if result.returncode != 0:
                print(f"❌ Error: Claude Code failed (exit code {result.returncode})")
                if result.stderr:
                    print(f"   stderr: {result.stderr[:200]}")
                return False

            # Check if Claude returned any output
            if result.stdout is None:
                print(f"❌ Error: Claude returned no output")
                if result.stderr:
                    print(f"   stderr: {result.stderr[:200]}")
                return False

            validation_sql = result.stdout.strip()

            # Save to file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Validation SQL for: {self.sql_file_path.name}\n")
                if self.jira_ticket:
                    f.write(f"-- JIRA: {self.jira_ticket}\n")
                f.write(f"-- Generated by SQL Analyzer\n\n")
                f.write(validation_sql)

            print(f"✅ Validation query saved to: {output_file}")
            return True

        except subprocess.TimeoutExpired:
            print("❌ Error: Validation query generation timed out")
            return False
        except Exception as e:
            print(f"❌ Error generating validation query: {e}")
            return False

    def generate_documentation(self) -> bool:
        """
        Generate README documentation using Claude Code.

        Returns:
            True if successful, False otherwise
        """
        print("\n📝 Generating README documentation...")

        try:
            output_file = self.documentation_dir / f"{self.base_name}_README.md"

            # Build comprehensive prompt
            prompt = f"""Analyze this SQL query and generate comprehensive README documentation.

SQL FILE: {self.sql_file_path.name}
{f"JIRA CONTEXT: {self.jira_context}" if self.jira_context else ""}

SQL QUERY:
```sql
{self.sql_content}
```

Generate a detailed README.md that includes:
1. Overview - What this query does and its business purpose
2. JIRA Context - Link to ticket and requirements
3. Tables Used - List all source tables with descriptions
4. Key Metrics - Explain calculated metrics and dimensions
5. Business Logic - Explain complex calculations, CTEs, and logic
6. Data Flow - Describe how data flows through the query
7. Filters & Conditions - Document key WHERE clauses and date ranges
8. Output Schema - List output columns with descriptions
9. Dependencies - Note any dependent tables or queries
10. Usage Examples - How to run and interpret results

Format as markdown. Be clear and thorough.
Output ONLY the markdown documentation, no explanatory text before or after."""

            # Call Claude Code (pass prompt via stdin to avoid command-line length limits)
            # Unset CLAUDECODE env var to allow nested execution
            env = os.environ.copy()
            env.pop('CLAUDECODE', None)

            # Increased timeout for complex queries (was 120s)
            result = subprocess.run(
                [self.claude_cli, 'code', '--print', '-'],
                input=prompt,
                capture_output=True,
                text=True,
                timeout=240,
                env=env
            )

            if result.returncode != 0:
                print(f"❌ Error: Claude Code failed (exit code {result.returncode})")
                if result.stderr:
                    print(f"   stderr: {result.stderr[:200]}")
                return False

            # Check if Claude returned any output
            if result.stdout is None:
                print(f"❌ Error: Claude returned no output")
                if result.stderr:
                    print(f"   stderr: {result.stderr[:200]}")
                return False

            documentation = result.stdout.strip()

            # Save to file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(documentation)

            print(f"✅ Documentation saved to: {output_file}")
            return True

        except subprocess.TimeoutExpired:
            print("❌ Error: Documentation generation timed out")
            return False
        except Exception as e:
            print(f"❌ Error generating documentation: {e}")
            return False

    def generate_improvements(self) -> bool:
        """
        Generate improvement suggestions using Claude Code.

        Returns:
            True if successful, False otherwise
        """
        print("\n🚀 Generating improvement suggestions...")

        try:
            output_file = self.improvements_dir / f"{self.base_name}_improvements.md"

            # Build comprehensive prompt
            prompt = f"""Analyze this SQL query and suggest improvements and optimizations.

SQL FILE: {self.sql_file_path.name}
{f"JIRA CONTEXT: {self.jira_context}" if self.jira_context else ""}

SQL QUERY:
```sql
{self.sql_content}
```

Generate a detailed improvements document that includes:

1. Performance Optimizations
   - Index recommendations
   - Query restructuring suggestions
   - Partition strategy improvements
   - Join optimization opportunities
   - Subquery vs CTE trade-offs

2. Code Quality
   - SQL best practices violations
   - Readability improvements
   - Naming convention suggestions
   - Comment and documentation gaps

3. Maintainability
   - Hardcoded values that should be parameters
   - Complex logic that could be simplified
   - Reusability improvements
   - Error handling suggestions

4. Data Quality
   - Missing null checks
   - Data type consistency issues
   - Business logic edge cases
   - Potential data quality issues

5. Scalability Concerns
   - Handling data growth
   - Query timeout risks
   - Resource usage concerns

For each suggestion:
- Explain the issue
- Show example of improvement
- Estimate impact (High/Medium/Low)
- Note any trade-offs

Format as markdown with clear sections.
Output ONLY the markdown improvements document, no explanatory text before or after."""

            # Call Claude Code (pass prompt via stdin to avoid command-line length limits)
            # Unset CLAUDECODE env var to allow nested execution
            env = os.environ.copy()
            env.pop('CLAUDECODE', None)

            # Increased timeout for complex queries (was 120s)
            result = subprocess.run(
                [self.claude_cli, 'code', '--print', '-'],
                input=prompt,
                capture_output=True,
                text=True,
                timeout=240,
                env=env
            )

            if result.returncode != 0:
                print(f"❌ Error: Claude Code failed (exit code {result.returncode})")
                if result.stderr:
                    print(f"   stderr: {result.stderr[:200]}")
                return False

            # Check if Claude returned any output
            if result.stdout is None:
                print(f"❌ Error: Claude returned no output")
                if result.stderr:
                    print(f"   stderr: {result.stderr[:200]}")
                return False

            improvements = result.stdout.strip()

            # Save to file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(improvements)

            print(f"✅ Improvements saved to: {output_file}")
            return True

        except subprocess.TimeoutExpired:
            print("❌ Error: Improvements generation timed out")
            return False
        except Exception as e:
            print(f"❌ Error generating improvements: {e}")
            return False

    def analyze(self) -> bool:
        """
        Run the complete analysis workflow.

        Returns:
            True if all steps completed successfully, False otherwise
        """
        print("=" * 70)
        print("🤖 SQL Analyzer - Powered by Claude Code + Atlassian MCP")
        print("=" * 70)
        print()

        # Step 1: Validate file
        if not self.validate_file():
            return False

        # Step 2: Read SQL file
        if not self.read_sql_file():
            return False

        # Step 3: Extract JIRA ticket
        self.extract_jira_ticket()

        # Step 4: Fetch JIRA context (non-blocking)
        self.fetch_jira_context()

        # Step 5: Ensure output directories exist
        self.ensure_output_directories()

        # Step 6: Generate artifacts
        results = {
            'validation': self.generate_validation_query(),
            'documentation': self.generate_documentation(),
            'improvements': self.generate_improvements()
        }

        # Summary
        print("\n" + "=" * 70)
        print("📊 Analysis Summary")
        print("=" * 70)
        print(f"SQL File: {self.sql_file_path}")
        if self.jira_ticket:
            print(f"JIRA Ticket: {self.jira_ticket}")
        print(f"\nResults:")
        print(f"  Validation Query:  {'✅ Success' if results['validation'] else '❌ Failed'}")
        print(f"  Documentation:     {'✅ Success' if results['documentation'] else '❌ Failed'}")
        print(f"  Improvements:      {'✅ Success' if results['improvements'] else '❌ Failed'}")
        print()

        success = all(results.values())
        if success:
            print("🎉 Analysis completed successfully!")
            print(f"\nGenerated files:")
            print(f"  📁 {self.validation_dir / f'{self.base_name}_validation.sql'}")
            print(f"  📁 {self.documentation_dir / f'{self.base_name}_README.md'}")
            print(f"  📁 {self.improvements_dir / f'{self.base_name}_improvements.md'}")
        else:
            print("⚠️  Analysis completed with some failures")

        print("=" * 70)
        return success


def main():
    """Main entry point for the SQL analyzer script."""
    parser = argparse.ArgumentParser(
        description='Analyze SQL queries using Claude Code + Atlassian MCP',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python analyze_sql.py queries/wam_site_performance.sql
  python analyze_sql.py queries/my_query.sql

This script generates:
  - Validation SQL (generated/validation/)
  - README documentation (generated/documentation/)
  - Improvement suggestions (generated/improvements/)
        """
    )

    parser.add_argument(
        'sql_file',
        type=str,
        help='Path to the SQL file to analyze'
    )

    parser.add_argument(
        '--version',
        action='version',
        version='SQL Analyzer 1.0.0'
    )

    args = parser.parse_args()

    # Run analysis
    analyzer = SQLAnalyzer(args.sql_file)
    success = analyzer.analyze()

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
