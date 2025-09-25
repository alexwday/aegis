
import sys
import io
sys.path.insert(0, "src")

# Redirect stdout before importing
old_stdout = sys.stdout
sys.stdout = io.StringIO()

from aegis.utils.prompt_loader import _load_global_prompts

# Now call with a nonexistent prompt
result = _load_global_prompts(["this_does_not_exist"])

# Get the output
output = sys.stdout.getvalue()
sys.stdout = old_stdout

print("Output:", repr(output))
print("Warning found:", "Warning" in output)
