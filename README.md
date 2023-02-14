# cross-account-s3-copy-python
Contains policies to use and python multipart code for transfer b/w two accounts.
Keep in mind the analogy of source account and destination account otherwise you will end up with wrong configuration.

Main steps are:

- Source account bucket policy change.
- Source account bucket KMS keys policy change if present.
- Destination account IAM user with the defined policy.
- AWS cli to test the connection.  (aws cli cp <source-account-bucket> <destination-account-bucket>
- If works successfully and you need to send large files, then update the python file accordingly and run.
- This won't take the additional cost except from transfer cost of AWS which is 0.09 per GB.
