import boto3

glue_client = boto3.client("glue")
s3_client = boto3.client("s3")

if __name__ == "__main__":

    for i in listaGeral:
        glue_args = {
            'class': 'GlueApp',
            'date': i,
            'dest_bucket': 'captalys-analytics-land-production',
            'domain': 'brl',
            'file': 'aquisicao',
            'source_bucket': 'captalys-analytics-adms-production',
            'technology': 'sftp',
        }

        args = {
            '--class': glue_args['class'],
            '--date': glue_args['date'],
            '--dest_bucket': glue_args['dest_bucket'],
            '--domain': glue_args['domain'],
            '--file': glue_args['file'],
            '--source_bucket': glue_args['source_bucket'],
            '--technology': glue_args['technology']
        }

        print(args)

        glue_client.start_job_run(
            JobName='datalake-sftp-down-brl',
            Arguments=args
        )
