#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
import selenium.webdriver.common.keys
from selenium import webdriver
import pandas as pd
import numpy as np
import time
import re
from collections import Counter
import argparse
from joblib import Parallel, delayed
from multiprocessing import cpu_count
import os

os.makedirs("scraper",exist_ok=True)
print("Output source accessible...")

stop_list = [
"PMP",
"PMI-ACP"
"polygraph",    
"drug",    
"bachelor",
"bachelors",
"bachelor's",
"master",
"masters",
"master's",
"years ",
"older",
"school",
"diploma",
"certified",
"certification",
"certifications",
"certification's",
"certificate",
"driver",
"driving ",
"driver license",
"driving license",
"drivers license",
"driver's license",
"ged",
"work authorization",
"secret clearence",
"skill",
"skills",
"phd",
"doctorate",
"ph.d",
"philosophy",
"mba",
"experience",
"in field",
"in-field",
"hands on",
"hands-on",
"knowledge",
"proficient,"
"proficiency,"
"fluency",
"fluent",
"secret",
"clearence",
"clear,ences"
"immigration",
"immigrate",
"immigrates",
"student ",
"interview",
"students",
"interviews",
"employer",
"employee",
"ssn",
"social security number",
"degree",
"degrees",
"degree's",
"location",
"Military",
"Contracts",
"Polygraph",
"Teaching",
"demo"]

scraped_job_list = []

error_list = {}

def multiple_page_scraper_usa(job_title,stop_point,job_limit=10000):
	
	job_title0 = job_title
	
	print("Scraping ",stop_point," jobs")
	
	options = FirefoxOptions() 
	options.add_argument("--headless")

	try:
		driver = webdriver.Firefox(executable_path="D:/IntellectFaces/REPO/RytFit-AI/scraper/geckodriver",options=options)
		driver.get('https://www.simplyhired.com/')
		
		driver.maximize_window()
		button = driver.find_element_by_xpath('//*[@id="SearchForm-whatInput"]')
		button.send_keys(job_title)
		
		error_list[job_title] = 0
		
		button = driver.find_element_by_xpath("//*[@id='SearchForm-whereInput']")
		button.clear()

		button = driver.find_element_by_xpath("/html/body/main/div/div/div/section[1]/form/div/button")   
		button.click()
		
	except:
		driver.close()
		with open(f"error log.txt","a") as p:
			p.write(f"Error happened for {job_title}\n")
		return "ERROR 404"

	try:
		time.sleep(5)
		tot_jobs = int(driver.find_element_by_xpath("//span[@class='CategoryPath-total']").text.replace(',',''))
	except: 
		print(f"ERROR Happened for {job_title}")
			 
		error_list[job_title] = "FAILED TO GET TOTAL JOBs"

		driver.close()

		with open(f"error log.txt","a") as p:
			p.write(f"Error happened for {job_title}\n")
		return "ERROR 404"
		
	print(f'total available jobs for {job_title} are {tot_jobs}')
	
	job_list = driver.find_elements_by_xpath('//div[@class="jobposting-title-container"]')
	
	time.sleep(3)
	
	if(tot_jobs>10):
		tot_jobs = tot_jobs-10
	
	if(tot_jobs<stop_point):
		stop_point=tot_jobs
	
	count = 0 
	
	try:
		for n in range(0,job_limit):
			
			if(tot_jobs<stop_point):
				stop_point=tot_jobs
				
			if (count% 50 == 0):
				print(f'{count} done!')
			
			if ((count)>=stop_point):
				pd.DataFrame(scraped_job_list,columns=["job_title",'location','skills','jd','url']).to_csv(f"D:/IntellectFaces/REPO/RytFit-AI/scraper/{job_title0}.csv",index=False)
				driver.close()
				return "SUCCESS"
			
			if((count % (len(job_list)) == 0)):
				
				try:
					time.sleep(5)
					tot_jobs = int(driver.find_element_by_xpath("//span[@class='CategoryPath-total']").text.replace(',',''))
				except:
					print(f"ERROR Happened for {job_title} in page number {count/len(job_list)}")
			
					# with open(f'log-for-{job_title0}.txt','a') as t:
					#     t.write(f"ERROR Happened for {job_title}")
					#     t.close()

					error_list[job_title] = "FAILED TO GET TOTAL JOBs"
					driver.close()
					return "ERROR 404"
				
				job_list = driver.find_elements_by_xpath('//div[@class="jobposting-title-container"]')
				time.sleep(5)
				
				if(tot_jobs>10):
					tot_jobs = tot_jobs-10
				
				try:
					job_list[0].click()
					time.sleep(5)
				except:
					count = stop_point
					
					break
				
				try:
					try:
						job_title=job_list[0].text.split(" - ")[0]
						
						try:
							url = driver.find_elements_by_xpath('//a[@class="SerpJob-link card-link"]')[0].get_attribute("href")
						except:
							url= None
						
						location=driver.find_elements_by_xpath('//div[@class="viewjob-labelWithIcon"]')[1].text
						
						job_skills = driver.find_element_by_xpath(".//div[@class='viewjob-section viewjob-qualifications viewjob-entities']").text.split("\n")[1:]
						
						try:
							
							jd = driver.find_element_by_xpath("/html/body/div[9]/div/aside/div/div[3]/div/div[2]").text
							result = "JD SUCCESS"
						except:
							jd = None
							result = "JD FAIL"
						
						
						job_skill = []

						for skill in job_skills:
							boolean = False
							for stop in stop_list:
								if re.search(fr"\b{stop}\b", skill,flags=re.I):
									boolean = True
									break
							if not boolean:
								job_skill.append(skill)

						# with open(f'log-for-{job_title0}.txt','a') as t:
						#     t.write(f'{count+1}) {job_title} ----- {location} ---- {job_skill} && {result}' + '\n')
						#     t.close()

						print(f'{count+1}) {job_title} ----- {location} ---- {job_skill} && {result}')   

						scraped_job_list.append((job_title,location,job_skill,jd,url))
					
					except:
						job_title=job_list[0].text.split(" - ")[0]
						location=None
						job_skill=[]
						jd = None
						url = None

						# with open(f'log-for-{job_title0}.txt','a') as t:
						#     t.write(f'{count+1}) {job_title} ----- {location} ---- {job_skill} ID FAILED' + f'\nERROR Happened for {job_title}')
						#     t.close()
							
						print(f'{count+1}) {job_title} ----- {location} ---- {job_skill} JD FAILED')   
						scraped_job_list.append((job_title,location,job_skill,jd,url))
				
				except:
					count+=1
					error_list[job_title]+=1
					
					# with open(f'log-for-{job_title0}.txt','a') as t:
					#     t.write(f"ERROR Happened for {job_title} in the major except class 1")
					#     t.close()
					
					break
					
					
				count+=1
				
			else:
				
				for i in range(1,len(job_list)):
					
					if (count%50==0):
						print(f'{count} done!')
						
					if ((count)>=(stop_point)):
						pd.DataFrame(scraped_job_list,columns=["job_title",'location','skills','jd','url']).to_csv(f"D:/IntellectFaces/REPO/RytFit-AI/scraper/{job_title0}.csv",index=False)
						driver.close()
						
						return "SUCCESS"
					
					time.sleep(5)
					
					if((i == (len(job_list)-1)) and (count != (stop_point-1))): 
						
						try:
							job_list[i].click()
							time.sleep(5)
						except:
							count = stop_point
							# with open(f'log-for-{job_title0}.txt','a') as t:
							#     t.write(f"ERROR Happened CLICK")
							#     t.close()
							break
						
						
						try:
							
							try:
								
								job_title=job_list[i].text.split(" - ")[0]
								location=driver.find_elements_by_xpath('//div[@class="viewjob-labelWithIcon"]')[1].text
								
								
								try:
									url = driver.find_elements_by_xpath('//a[@class="SerpJob-link card-link"]')[i].get_attribute("href")
								except:
									url= None
								
								
								job_skills = driver.find_element_by_xpath(".//div[@class='viewjob-section viewjob-qualifications viewjob-entities']").text.split("\n")[1:]
					
								try:
									jd = driver.find_element_by_xpath("/html/body/div[9]/div/aside/div/div[3]/div/div[2]").text
									result = "JD SUCCESS"
								except:
									jd = None
									result = "JD FAIL"


								job_skill = []

								for skill in job_skills:
									boolean = False
									for stop in stop_list:
										if re.search(fr"\b{stop}\b", skill,flags=re.I):
											boolean = True
											break
									if not boolean:
										job_skill.append(skill)

								# with open(f'log-for-{job_title0}.txt','a') as t:
								#     t.write(f'{count+1}) {job_title} ----- {location} ---- {job_skill} && {result}' + '\n')
								#     t.close()

								print(f'{count+1}) {job_title} ----- {location} ---- {job_skill} && {result}')   

								scraped_job_list.append((job_title,location,job_skill,jd,url))   

								element = driver.find_element_by_xpath('//a[@class="Pagination-link next-pagination"]')
								driver.execute_script("arguments[0].click();", element)

								time.sleep(5)
							
							except:

								job_title=job_list[i].text.split(" - ")[0]
								location=None
								job_skill=[]
								jd = None
								url=None
								scraped_job_list.append((job_title,location,job_skill,jd,url))

								# with open(f'log-for-{job_title0}.txt','a') as t:
								#     t.write(f'{count+1}) {job_title} ----- {location} ---- {job_skill} JD FAILED' + f'\nERROR Happened for {job_title} in the major except class 0')
								#     t.close()
								
								print(f'{count+1}) {job_title} ----- {location} ---- {job_skill} JD FAILED')   

								element = driver.find_element_by_xpath('//a[@class="Pagination-link next-pagination"]')
								driver.execute_script("arguments[0].click();", element)

								time.sleep(5)
							
						except:
							count+=1
							error_list[job_title]+=1
							# with open(f'log-for-{job_title0}.txt','a') as t:
							#     t.write(f"ERROR Happened for {job_title} in the major except class 2")
							#     t.close()
							break
							
					elif((i == (len(job_list)-1)) and (count == (stop_point-1))):
						
						try:
							job_list[i].click()
							time.sleep(5)
						except:
							count = stop_point
							
							# with open(f'log-for-{job_title0}.txt','a') as t:
							#     t.write(f"ERROR Happened CLICK")
							#     t.close()
							break
							
						try:
							
							try:
								job_title=job_list[i].text.split(" - ")[0]
								
								try:
									url = driver.find_elements_by_xpath('//a[@class="SerpJob-link card-link"]')[i].get_attribute("href")
								except:
									url= None
								
								location=driver.find_elements_by_xpath('//div[@class="viewjob-labelWithIcon"]')[1].text
								
								job_skills = driver.find_element_by_xpath(".//div[@class='viewjob-section viewjob-qualifications viewjob-entities']").text.split("\n")[1:]
					
								try:
									jd = driver.find_element_by_xpath("/html/body/div[9]/div/aside/div/div[3]/div/div[2]").text
									result = "JD SUCCESS"
								except:
									jd = None
									result = "JD FAIL"


								job_skill = []

								for skill in job_skills:
									boolean = False
									for stop in stop_list:
										if re.search(fr"\b{stop}\b", skill,flags=re.I):
											boolean = True
											break
									if not boolean:
										job_skill.append(skill)

								# with open(f'log-for-{job_title0}.txt','a') as t:
								#     t.write(f'{count+1}) {job_title} ----- {location} ---- {job_skill} && {result}' + '\n')
								#     t.close()

								print(f'{count+1}) {job_title} ----- {location} ---- {job_skill} && {result}')   

								scraped_job_list.append((job_title,location,job_skill,jd,url))
								
								print("End of last page")
								count+=1
								break
							
							except:
								job_title=job_list[i].text.split(" - ")[0]
								location=None
								job_skill=[]
								jd = None
								url = None
								scraped_job_list.append((job_title,location,job_skill,url))

								# with open(f'log-for-{job_title0}.txt','a') as t:
								#     t.write(f'{count+1}) {job_title} ----- {location} ---- {job_skill} JD FAILED' + f'\nERROR Happened for {job_title} in the major except class 4')
								#     t.close()

								print(f'{count+1}) {job_title} ----- {location} ---- {job_skill} JD FAILED')   
								print("End of last page")
								count+=1
								break
						
						except:
							count+=1
							error_list[job_title]+=1
							
							# with open(f'log-for-{job_title0}.txt','a') as t:
							#     t.write(f"ERROR Happened for {job_title} in the major except class 5")
							#     t.close()
							
							break
				
					else:
						
						try:
							job_list[i].click()
							time.sleep(5)
						except:
							count = stop_point
							# with open(f'log-for-{job_title0}.txt','a') as t:
							#     t.write(f"ERROR Happened CLICK")
							#     t.close()
							break
						
						try:
							
							try:
								job_title=job_list[i].text.split(" - ")[0]
								location=driver.find_elements_by_xpath('//div[@class="viewjob-labelWithIcon"]')[1].text
								
								try:
									url = driver.find_elements_by_xpath('//a[@class="SerpJob-link card-link"]')[i].get_attribute("href")
								except:
									url= None
								
								job_skills = driver.find_element_by_xpath(".//div[@class='viewjob-section viewjob-qualifications viewjob-entities']").text.split("\n")[1:]
					
								try:
									jd = driver.find_element_by_xpath("/html/body/div[9]/div/aside/div/div[3]/div/div[2]").text
									result = "JD SUCCESS"
								except:
									jd = None
									result = "JD FAIL"


								job_skill = []

								for skill in job_skills:
									boolean = False
									for stop in stop_list:
										if re.search(fr"\b{stop}\b", skill,flags=re.I):
											boolean = True
											break
									if not boolean:
										job_skill.append(skill)

								# with open(f'log-for-{job_title0}.txt','a') as t:
								#     t.write(f'{count+1}) {job_title} ----- {location} ---- {job_skill} && {result}' + '\n')
								#     t.close()

								print(f'{count+1}) {job_title} ----- {location} ---- {job_skill} && {result}')   

								scraped_job_list.append((job_title,location,job_skill,jd,url))
							
							except:
								job_title=job_list[i].text.split(" - ")[0]
								location=None
								jd = None
								url = None
								job_skill=[]

								print(f'{count+1}) {job_title} ----- {location} ---- {job_skill} JD FAILED')   
								scraped_job_list.append((job_title,location,job_skill,jd,url))
								
						except:
							count+=1
							error_list[job_title]+=1
							
							# with open(f'log-for-{job_title0}.txt','a') as t:
							#     t.write(f"ERROR Happened for {job_title} in the major except class 6")
							#     t.close()
								
							break
					count+=1

	except:
		
		print("Error!!!")     
		return "ERROR 404"

titles = ['General manager',
 'operations managers',
 'Legislators',
 'Advertising and promotions managers',
 'Marketing managers',
 'Sales managers',
 'Public relations',
 'fundraising managers',
 'Operations specialties managers',
 'Administrative services manager',
 'facilities manager',
 'information systems managers',
 'Financial managers',
 'Industrial production managers',
 'Purchasing managers',
 'Transportation managers',
 'storage managers',
 'distribution managers',
 'Compensation and benefits managers',
 'Human resources managers',
 'Training and development managers',
 'Construction managers',
 'Education and childcare administrators',
 'Architectural',
 'engineering managers',
 'Food service managers',
 'Gambling managers',
 'Lodging managers',
 'Medical and health services managers',
 'Natural sciences managers',
 'Postmasters',
 'mail superintendents',
 'real estate',
 'Funeral home managers',
 'Personal service managers',
 'entertainment manager',
 'recreation managers',
 'Compliance officers',
 'Cost estimators',
 'Human resources specialists',
 'Farm labor contractors',
 'Labor relations specialists',
 'Logisticians',
 'Management analysts',
 'event planners',
 'Fundraisers',
 'Training and development specialists',
 'Market research analysts',
 'marketing specialists',
 'Financial specialists',
 'Accountants',
 'auditors',
 'Property appraisers',
 'Budget analysts',
 'Credit analysts',
 'Personal financial advisors',
 'Credit counselors',
 'Loan officers',
 'financial risk specialists',
 'Financial and investment analysts',
 'Computer and information analysts',
 'Computer systems analysts',
 'Information security analysts',
 'Computer and information research scientists',
 'Computer network support specialists',
 'Computer user support specialists',
 'Database administrators',
 'Database architects',
 'network architects',
 'digital interface designers',
 'Mathematicians',
 'Operations research analysts',
 'Statisticians',
 'Architects',
 'Landscape architects',
 'Cartographers',
 'photogrammetrists',
 'Surveyors',
 'Aerospace engineers',
 'Agricultural engineers',
 'Bio engineers',
 'biomedical engineers',
 'Chemical engineers',
 'Civil engineers',
 'Computer hardware engineers',
 'Electrical engineers',
 'Electronics engineers',
 'Environmental engineers',
 'Industrial engineers',
 'Health and safety engineers',
 'Marine engineers',
 'naval architects',
 'Materials engineers',
 'Mechanical engineers',
 'Mining engineer',
 'geological engineers',
 'Nuclear engineers',
 'Petroleum engineers',
 'Architectural drafters',
 'civil drafters']
		

Parallel(n_jobs=cpu_count() * 2)(delayed(multiple_page_scraper_usa)(g,2000) for g in titles)




	
		
		