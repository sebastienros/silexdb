; Unshipped analyzer release tracking

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|------
SILEX001 | Ownership | Error | Copy-sensitive marked values cannot be copied from existing storage or passed by value.
SILEX002 | Ownership | Error | Direct acquisitions of marked owned values require disposal or an explicit transfer.
SILEX003 | Ownership | Error | Marked owned members must be released by the containing type's Dispose path.
