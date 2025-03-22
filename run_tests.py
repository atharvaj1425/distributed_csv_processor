import unittest
import sys

if __name__ == "__main__":
    # Load tests from test_integration.py
    test_suite = unittest.defaultTestLoader.discover('.', pattern='test_integration.py')
    
    # Run tests
    result = unittest.TextTestRunner(verbosity=2).run(test_suite)
    
    # Return appropriate exit code (0 for success, 1 for failure)
    sys.exit(not result.wasSuccessful())