from bs4 import BeautifulSoup
import requests
import pandas as pd

import math


def scrape_page(url):
    # response = requests.get(url, params={"cursor": cursor})
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    return soup

def get_cursor(soup, next_page_num):
    cursor = soup.find('a', {'class':'chakra-link css-1wxsdwr', 'aria-label': f'page {next_page_num}'})
    if cursor:
        return cursor.get('href')
    else:
        return None
    

def get_job_links(url, list_elems):
    job_links = []
    for item in list_elems:
        job_key = item.div.get('data-jobkey')
        job_link = f'{url}&job={job_key}'
        job_links.append(job_link)
    return job_links


def scrape_one_page(url, soup):
    ul = soup.find_all('ul', id = 'job-list')
    li = ul[0].find_all('li', {'class':'css-0'})
    job_links = get_job_links(url, li)
    data_list = []
    for job_link in job_links:
        print(job_link)
        job = scrape_page(job_link)
        job_title = job.h2.text.strip()
        company = job.find('span', {'data-testid': 'viewJobCompanyName'}).text if job.find('span', {'data-testid': 'viewJobCompanyName'}) else ""
        location = job.find('span', {'data-testid': 'viewJobCompanyLocation'}).text if job.find('span', {'data-testid': 'viewJobCompanyLocation'}) else ""
        job_type = job.find('span', {'data-testid': 'viewJobBodyJobDetailsJobType'}).text if job.find('span', {'data-testid': 'viewJobBodyJobDetailsJobType'}) else ""
        salary = job.find('span', {'data-testid': 'viewJobBodyJobCompensation'}).text if job.find('span', {'data-testid': 'viewJobBodyJobCompensation'}) else ""
        posted_on = job.find('span', {'data-testid': 'viewJobBodyJobPostingTimestamp'}).text if job.find('span', {'data-testid': 'viewJobBodyJobPostingTimestamp'}) else ""
        job_qualification = job.find('div', {'data-testid': 'viewJobQualificationsContainer'})\
                                .find('ul')
        if job_qualification:
            job_qualification = list(job.find('div', {'data-testid': 'viewJobQualificationsContainer'})\
                                    .find('ul').strings)
            job_qualification = [x.strip() for x in job_qualification]
            # job_qualification = "\n".join(job_qualification)
        else:
            job_qualification = ""

        job_description = job.find('div', {'data-testid':'viewJobBodyJobFullDescriptionContent'})
        if job_description:
            job_description = list(job.find('div', {'data-testid':'viewJobBodyJobFullDescriptionContent'}).strings)
            job_description = [x.strip() for x in job_description]
            # job_description = "\n".join(job_description)
        else:
            job_description = ""
        data = {
                'job_title':job_title,
                'company':company,
                'location': location, 
                'job_type': job_type, 
                'salary':salary, 
                'posted_on':posted_on, 
                'job_qualification':job_qualification, 
                'job_description': job_description
                }
        data_list.append(data)
        # break
    return data_list
        

job = 'Data Engineer'
location = 'texas'
data_up_to = 7  #last seven days

url = f'https://www.simplyhired.com/search?q={job}&l={location}&t={data_up_to}'
next_page = url

soup = scrape_page(url)
number_of_jobs = soup.find('div', {'data-testid':'headerSerpJobCount'}).p.text
number_per_page = len(soup.find_all('ul', {'id':'job-list'})[0]\
        .find_all('li', {'class':'css-0'}))
number_of_pages = math.ceil(int(number_of_jobs)/int(number_per_page))
print({'number_of_jobs': number_of_jobs, 'number_per_page': number_per_page, 'number_of_pages': number_of_pages})

i=1
df = pd.DataFrame(columns=['job_title', 'company', 'location', 'job_type', 'salary', 'posted_on', 'job_qualification', 'job_description'])
while next_page != None:
    print('Page Number:', i, ' Page Link: ', next_page)
    soup = scrape_page(next_page)
    page_data = scrape_one_page(next_page, soup)
    df = df.append(page_data)

    i = i+1
    next_page = get_cursor(soup, i+1)
    
print("Complete")
df.to_csv('simply_hired.csv',index=False)
df.head()
