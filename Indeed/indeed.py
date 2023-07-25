import yaml
import boto3
import pandas as pd
import io
import os
import re
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService 
from webdriver_manager.chrome import ChromeDriverManager 
from scrapy.selector import Selector
from pandas import DataFrame
from thefuzz import fuzz
from tenacity import retry, stop_after_attempt
from datetime import datetime, timedelta

with open(os.path.join(os.getcwd(), '../jobs.yml'), 'r') as yaml_file:
    # Load the YAML content
    jobs_config = yaml.safe_load(yaml_file)
    
    
def create_jobs(s3_client,bucket_name:str,job_site:str)->None:
    try:
        folders_list = s3_client.list_objects_v2(Bucket=bucket_name)['Contents']
        folders_list = [folder['Key'].split('/')[0] for folder in folders_list if folder['Key'].split('/')[0]=='indeed']
        if job_site not in folders_list:
            s3_client.put_object(Bucket=bucket_name, Key=job_site+'/')
        else:
            print(f'Folder already exists.')
    except Exception as e:
        print(f'Exception: {e} occured.')
    return
    
    
def write_dataframe_to_s3(s3_client, bucket_name: str,df:DataFrame)->None:
    try:
        create_jobs(s3_client,bucket_name, list(jobs_config.keys())[0])
        with io.StringIO() as csv_buffer:
            filename = str(datetime.today().date())
            df.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(
                Bucket = bucket_name,
                Key = f'indeed/{filename}.csv',
                Body = csv_buffer.getvalue()
            )
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")    
            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}") 
    except Exception as e:
        print(f'Exception: {e}')
        
        
        
def check_exists_by_xpath(driver, xpath_expression):
    try:
        driver.find_element(By.XPATH,xpath_expression).click()
    except NoSuchElementException:
        return False
    return True
    
    
def calculate_date_from_string(input_string):
    print(input_string)
    if input_string is not None:
        string_list = input_string.split(' ')
        today = datetime.today()
        if "days" in string_list or 'day' in string_list:
            days_ago = int(input_string.split()[1])
            date = today - timedelta(days=days_ago)
        else:
            # Default to today's date if the input format is not recognized
            date = today
        return date.date()
    else:
        return datetime.today()
        
        
def scrape(driver, s3_client):
    job_list_of_dict = []
    for job in jobs_config['jobs']:
        scrape = True
        driver.get('https://www.indeed.com')
        sleep(2)
        # driver.find_element(By.XPATH, "//div[@class='css-1hyl41o e1ttgm5y0']").click()
        what = driver.find_element(By.XPATH,"//input[@id='text-input-what']")
        what.send_keys(Keys.CONTROL+'a')
        what.send_keys(Keys.DELETE)
        what.send_keys(job)
        sleep(2)
        where = driver.find_element(By.XPATH,"//input[@id='text-input-where']")
        where.send_keys(Keys.CONTROL+'a')
        where.send_keys(Keys.DELETE)
        where.send_keys(jobs_config['location'])
        where.send_keys(Keys.ENTER)
        sleep(2)
        
        driver.find_element(By.ID, 'filter-dateposted').click()
        sleep(3)
        try:
            driver.find_element(By.XPATH,"//li[@class='yosegi-FilterPill-dropdownListItem'][2]").click()
        except Exception as e:
            continue
        # original_window = driver.current_window_handle
        while scrape:
            count = 0
            irrelevant_jobs = 0
            sel = Selector(text=driver.page_source)
            cards = sel.xpath("//ul[@class='jobsearch-ResultsList css-0']/li")
            page = 1
            driver.execute_script("window.open('');")
            for card in cards:
                job_position = card.xpath('.//div[@class="css-1m4cuuf e37uo190"]/h2/a/span/@title').extract_first()
                if fuzz.partial_ratio(job, job_position) < 75 or job_position is None:
                        irrelevant_jobs+=1
                        continue
                else:
                    irrelevant_jobs=0
                if irrelevant_jobs > 10:
                    scrape=False
                    break
                divs = card.xpath('.//div[@class="heading6 tapItem-gutter metadataContainer noJEMChips salaryOnly"]/div')
                if len(divs)==1:
                    if divs.xpath('.//svg/@aria-label').extract_first()=='Salary':
                        salary = divs.xpath('.//text()').extract_first()
                        job_type=''
                    if divs.xpath('.//svg/@aria-label').extract_first()=='Job type':
                        job_type = divs.xpath('.//text()').extract_first()
                        salary = ''
                elif len(divs)==2:
                    if divs[0].xpath('./@class').extract_first()=='metadata estimated-salary-container':
                        salary = divs.xpath('.//span/text()').extract_first()
                    else:
                        salary = divs.xpath(".//div/text()").extract_first()
                    job_type = divs[1].xpath('.//div/text()').extract_first()
                else:
                        job_type=''
                        salary = ''
                try:
                    job_link = card.xpath('.//div[@class="css-1m4cuuf e37uo190"]/h2/a/@href').extract_first()
                    match = re.search(r"jk=([^&]+)", job_link)
                    if match:
                        result = match.group(1)
                    url = f'https://indeed.com/viewjob?jk={result}'
                except Exception as e:
                        pass
                job_posted_date = card.xpath('.//span[@class="date"]/text()').extract_first()
                job_posted_date = calculate_date_from_string(job_posted_date)
                # driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[1])
                driver.get(url)
                sleep(2)
                job_sec_full_page_src = driver.page_source
                job_sel = Selector(text=job_sec_full_page_src)
                company = job_sel.xpath("//div[@data-company-name='true']/text()").extract_first()
                location = job_sel.xpath("//div[@class='css-39gvaf eu4oa1w0']/div[2]/div/text()").extract_first()
                job_location = job_sel.xpath("//div[@class='css-39gvaf eu4oa1w0']/div[3]/div/text()").extract_first()
                job_description = ''
                try:
                    job_description_list = job_sel.xpath("//div[@id='jobDescriptionText']/text()").extract()
                    if len(job_description_list)==0:
                        job_description_list = job_sel.xpath("//div[@id='jobDescriptionText']/div/p/text()").extract() 
                    if len(job_description_list)==0:
                        job_description_list = job_sel.xpath("//div[@id='jobDescriptionText']/div/div/text()").extract() 
                except:
                    job_description
                cleaned_description=''.join(job_description_list).replace('\n','')
                # driver.switch_to.window(driver.window_handles[0])
                count+=1
                print(f'Page: {page}')
                print(f'Count: {count}')
                print(f'Position: {job_position}\nCompany:{company}\nlocation:{location}\njob_location:{job_location}\nsalary_range:{salary}\n')
                print(f'URL: {url}')
                print(f'Job Type: {job_type}')
                print(f'Description: {cleaned_description}')
                job_dict = {
                    'Search_Keyword_for_job': job,
                    'URL': url,
                    'Job_Posted_Date': job_posted_date,
                    'Position': job_position,
                    'Company': company,
                    'Job_Location': location,
                    'Job_Location_Type': job_location,
                    'Salary_Range': salary,
                    'Job_Type': job_type,
                    'Job_Description': cleaned_description
                }
                job_list_of_dict.append(job_dict)
                # if count == 2:
                #     break
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            sleep(2)
            page+=1
            # scrape = False
            scrape = check_exists_by_xpath(driver, "//a[@aria-label='Next Page']")
            sleep(3)
        # break
    jobs_df = pd.DataFrame(job_list_of_dict)
    write_dataframe_to_s3(s3_client=s3_client,bucket_name='fuse-internal-analytics-jobs-scraping-dev', df=jobs_df)
    driver.close()
    return jobs_df
        
if __name__=='__main__':
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless=new')
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    driver.maximize_window()
    # Load the YAML content
    with open(os.path.join(os.getcwd(), '../config.yml'), 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
    s3_client = boto3.client('s3', aws_access_key_id=config['Credentials']['AccessKeyId'], aws_secret_access_key=config['Credentials']['SecretAccessKey'])
    df = scrape(driver=driver, s3_client = s3_client)

