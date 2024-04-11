import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
from loguru import logger

def gen_task_parameter_list(pNum: str,searchElement: str):
    pNum_list = [i for i in range(0,int(pNum))]
    para_list = [
        dict(pageNum=str(d),data_source=data_source,searchElement=searchElement) for d in pNum_list for data_source in [
            "job_104",
            "job_1111",
        ]]
    return para_list

def crawler_header():
    return {'User_Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36'}

def crawler_104(searchElement,pageNum):
    df = pd.DataFrame(columns=['company','title','custUrl',
                        'appearDate','jobCategory','salary',
                        'addressArea','manageResp','businessTrip','workPeriod',
                        'vacationPolicy','startWorkingDay','needEmp',
                        'workExp','edu','major',
                        'skill','specialty','other','jobDescription'])

    url = f'https://www.104.com.tw/jobs/search/?ro=0&keyword={searchElement}&page={pageNum}'
    res = requests.session().get(url, headers= crawler_header())
    soup = BeautifulSoup(res.text,'html.parser')
    
    company_list = soup.select('div[class="b-block__left"] ul[class="b-list-inline b-clearfix"] li a')
    title_list = soup.select('div[class="b-block__left"] h2[class="b-tit"] a')
    total_company_num = len(company_list)

    for i in range(0,total_company_num):
        # 公司名稱
        company_name = str(company_list[i].text).split('\n')[1].split(' ')[24]
        # 職位名稱
        title = title_list[i].text
        # hidden code
        hidden_code = title_list[i]['href'].split('job/')[1].split('?')[0]
        real_articleUrl = 'https://www.104.com.tw/job/ajax/content/' + hidden_code

        # get real content
        real_headers = {'Referer': 'https://www.104.com.tw/job/' + hidden_code,
    'User_Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36',
    }
        real_resArticle = requests.get(real_articleUrl, headers=real_headers)
        all_content = json.loads(real_resArticle.text)

        try:
            # 公司網址
            custUrl = all_content['data']['header']['custUrl']
            custUrl = custUrl.strip()

            # 更新日期
            appearDate = all_content['data']['header']['appearDate']
            appearDate = appearDate.strip()

            # 職務類別
            category = all_content['data']['jobDetail']['jobCategory']
            category_list = ','.join([a['description'].strip() for a in category])

            # 薪資
            salary = all_content['data']['jobDetail']['salary']
            salary = salary.strip()

            # 地址
            addressArea = all_content['data']['jobDetail']['addressArea']
            addressArea = addressArea.strip()

            # 管理責任
            manageResp = all_content['data']['jobDetail']['manageResp']
            manageResp = manageResp.strip()

            # 出差外派
            businessTrip = all_content['data']['jobDetail']['businessTrip']
            businessTrip = businessTrip.strip()

            # 工作時間
            workPeriod = all_content['data']['jobDetail']['workPeriod']
            workPeriod = workPeriod.strip()

            # 休假制度
            vacationPolicy = all_content['data']['jobDetail']['vacationPolicy']
            vacationPolicy = vacationPolicy.strip()

            # 到職日期
            startWorkingDay = all_content['data']['jobDetail']['startWorkingDay']
            startWorkingDay = startWorkingDay.strip()

            # 需求人數
            needEmp = all_content['data']['jobDetail']['needEmp']
            needEmp = needEmp.strip()

            ## 條件要求
            # 工作經歷
            workExp = all_content['data']['condition']['workExp']
            workExp = workExp.strip()

            # 學歷
            edu = all_content['data']['condition']['edu']
            edu = edu.strip()

            # 科系限制
            major = all_content['data']['condition']['major']
            if len(major) == 0:
                major = '不拘'
            else:
                major = ','.join([a.strip() for a in major])

            # 工作技能
            skill = all_content['data']['condition']['skill']
            if len(skill) == 0:
                skill= '不拘'
            else:
                skill = ','.join([a['description'].strip() for a in skill])

            # 擅長工具
            specialty = all_content['data']['condition']['specialty']
            if len(specialty) == 0:
                specialty = '不拘'
            else:
                specialty = ','.join([a['description'].strip() for a in specialty])
                specialty = specialty.replace('\u200b', '')

            # 其他
            other = all_content['data']['condition']['other']
            if other == '':
                other = '未填寫'
            else:
                other = other.strip().replace('\n',',')

            # 工作內容
            job_content = all_content['data']['jobDetail']['jobDescription']
            job_content = job_content.strip()
            job_content = job_content.replace('\n',' ')


            dictContent = {
                'company':company_name,
                'title':title,
                'custUrl':custUrl,
                'appearDate':appearDate,
                'jobCategory':category_list,
                'salary':salary,
                'addressArea':addressArea,
                'manageResp':manageResp,
                'businessTrip':businessTrip,
                'workPeriod':workPeriod,
                'vacationPolicy':vacationPolicy,
                'startWorkingDay':startWorkingDay,
                'needEmp':needEmp,
                'workExp':workExp,
                'edu':edu,
                'major':major,
                'skill':skill,
                'specialty':specialty,
                'other':other,
                'jobDescription':job_content
            }
            
            df_tmp = pd.DataFrame(dictContent, index=[0])
            df = pd.concat([df,df_tmp], ignore_index = True)
        except Exception as e:
            logger.error(e)
            return pd.DataFrame()
    
    return df
    

def crawler_1111(searchElement,pageNum):
    df = pd.DataFrame(columns=['company','title','custUrl',
                        'appearDate','jobCategory','salary',
                        'addressArea','jobType','workPeriod',
                        'vacationPolicy','startWorkingDay','needEmp',
                        'workExp','edu','major',
                        'skill','specialty','other','jobDescription'])
    
    url = f'https://www.1111.com.tw/search/job?ks={searchElement}&page={pageNum}'
    res = requests.session().get(url, headers= crawler_header())
    soup = BeautifulSoup(res.text,'html.parser')

    company_url_list = soup.select('div[class="title position0"]')
    total_company_num = len(company_url_list)

    try:
        for i in range(0,total_company_num):
            company_code = company_url_list[i].find_all('a')[0].get('href')
            company_url = 'https://www.1111.com.tw' + company_code
            res_c = requests.session().get(company_url, headers=crawler_header())
            soup_c = BeautifulSoup(res_c.text,'html.parser')
            
            # 公司名稱
            company_name = soup_c.select('div[class="ui_card_company"] span[class="title_7"]')
            company_name = company_name[0].text

            # 職稱
            title = soup_c.select('div[class="header_title_fixed"] h1')
            title = title[0].text
            

            # 公司網址
            custUrl = soup_c.select('div[class="ui_card_company"] a')
            custUrl = 'https://www.1111.com.tw' + custUrl[0].get('href')

            # 更新日期
            appearDate = soup_c.select('div[class="ui_card_top"] small[class="text-muted job_item_date body_4"]')
            appearDate = appearDate[0].text
            appearDate = appearDate.split(' ')[1]

            # 職務類別
            category_list = soup_c.select('div[class="mb-3"] a u')
            category_list = ','.join([u_tag.text.strip() for u_tag in category_list])

            # 薪資
            salary = soup_c.select('div[class="ui_items job_salary"] p[class="body_2"]')
            salary = salary[0].text

            # 地址
            addressArea = soup_c.select('div[class="body_2 description_info"] div[class="mb-3"] span[class="job_info_content"] u')
            addressArea = addressArea[0].text

            # 工作性質
            job_type = soup_c.select('div[class="ui_items job_type"] p[class="body_2"]')
            job_type = job_type[0].text

            # 工作時間
            workPeriod = soup_c.select('div[class="body_2 description_info"] span[class="job_info_content color_primary_4"]')
            workPeriod = workPeriod[0].text

            # 休假制度
            vacationPolicy = soup_c.find('span', class_='job_info_title', text='休假制度：')
            
            if vacationPolicy == None:
                vacationPolicy = 'None'
            else:
                vacationPolicy = vacationPolicy.find_next('span', class_="job_info_content color_primary_4")
                vacationPolicy = vacationPolicy.text.strip()

            # 到職日期
            startWorkingDay = soup_c.select('div[class="applicants mb-3 d-flex align-items-center"] div[class="d-flex align-items-center body_4"] span')
            startWorkingDay = startWorkingDay[0].text

            # 需求人數
            needEmp = soup_c.select('div[class="applicants mb-3 d-flex align-items-center"] span[class="body_4 applicants_data"]')
            needEmp = needEmp[0].text

            # 工作經驗
            workExp = soup_c.select('div[class="body_2 description_info"] li[class="body_2"] div[class="d-flex"] span[class="job_info_content"]')
            workExp = workExp[0].text.strip()

            # 學歷
            edu = soup_c.select('div[class="ui_items job_education"] p[class="body_2"]')
            edu = edu[0].text.strip()

            # 科系限制
            major = soup_c.find('span', class_='job_info_title', text='科系限制：')
            major = major.find_next('span', class_="job_info_content")
            tmp = major.find_all('a')
            if len(tmp) != 0:
                major = ','.join([a.text.strip() for a in tmp])
            else:
                major = ','.join([a.text.strip() for a in major])

            # 工作技能
            specialty_list = soup_c.select('div[class="secondary-tag-nounderline"]')
            if len(specialty_list) != 0:
                specialty = ','.join([u_tag.text.strip() for u_tag in specialty_list])
            else:
                specialty = 'None'

            # 專長
            skill = soup_c.find('span', class_='job_info_title', text='電腦專長：')
            if skill == None:
                skill = 'None'
            else:
                skill = skill.find_next('span', class_="job_info_content")
                skill = ','.join([a.text.strip() for a in skill.find_all('a')])

            # 其他
            other = soup_c.select('div[class="job_info_content"] div[class="ui_items_group"]')
            if len(other) == 0:
                other = 'None'
            else:
                other = other[0].text.strip()

            # 工作內容
            job_content = soup_c.select('div[class="body_2 description_info"]')
            job_content = job_content[0].text.strip()
            job_content = job_content.replace('\xa0',' ')

            dictContent = {
                'company':company_name,
                'title':title,
                'custUrl':custUrl,
                'appearDate':appearDate,
                'jobCategory':category_list,
                'salary':salary,
                'addressArea':addressArea,
                'jobType':job_type,
                'workPeriod':workPeriod,
                'vacationPolicy':vacationPolicy,
                'startWorkingDay':startWorkingDay,
                'needEmp':needEmp,
                'workExp':workExp,
                'edu':edu,
                'major':major,
                'skill':specialty,
                'specialty':skill,
                'other':other,
                'jobDescription':job_content
            }

            df_tmp = pd.DataFrame(dictContent, index=[0])
            df = pd.concat([df,df_tmp], ignore_index = True)

    except Exception as e:
        logger.error(e)
        return pd.DataFrame()
    return df

def crawler(parameter) -> pd.DataFrame:
    logger.info(parameter)
    pageNum = parameter.get("pageNum", "")
    data_source = parameter.get("data_source", "") # 104 or 1111
    searchElement = parameter.get("searchElement", "")

    if data_source == "job_104":
        df = crawler_104(searchElement, pageNum)
    elif data_source == "job_1111":
        df = crawler_1111(searchElement, pageNum)
    return df


if __name__ == "__main__":
    searchElement = '資料工程師'
    pageNum = 5
