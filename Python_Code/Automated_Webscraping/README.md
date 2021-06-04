This folder is code created to automate the scraping of websites based of of values stored in a dynamodb database.
The template for each database entry can be seen below:

  {'url':'','articleTag':'','suburl':'','secondaryArticleTag':'','urlOverlap':'',freq:} (The values must be field in based on the website you want to scrape)
* scrape.py is the lambda function that is triggered by an sqs
* trigger.py is code that is run on a cronjob on an ec2 instance which checks to see if any of the sources in the database need to be scraped based on the freq value.
