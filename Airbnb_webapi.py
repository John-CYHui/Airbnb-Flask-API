from flask import Flask, request,jsonify
import json
import sqlite3

app = Flask(__name__)
app.config['DEBUG'] = True

#Do NOT put functions/statement outside functions

# Show your student ID
@app.route('/mystudentID/', methods=['GET'])
def my_student_id():    
    response={"studentID": "20012461G"}
    return jsonify(response), 200, {'Content-Type': 'application/json'}

@app.route('/airbnb/reviews/', methods=['GET'])
def get_all_reviews():
    conn = sqlite3.connect('airbnb.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    #If Start and End date are specified
    if 'start' in request.args.keys() and 'end' in request.args.keys():
        start_date = str(request.args['start'])
        end_date = str(request.args['end'])
        
        cursor.execute('''SELECT comment,
                                 rid,
                                 datetime,
                                 accommodation_id,
                                 rname
                            FROM review
                                 NATURAL JOIN
                                 reviewer
                            WHERE review.rid = reviewer.rid AND 
                                 DATE(datetime) BETWEEN "%s" AND "%s"
                            ORDER BY DATE(datetime) DESC, rid ASC;''' %(start_date,end_date))
    #If only Start date is specified
    elif 'start' in request.args.keys():
        start_date = str(request.args['start'])        
        cursor.execute('''SELECT comment,
                                 rid,
                                 datetime,
                                 accommodation_id,
                                 rname
                            FROM review
                                 NATURAL JOIN
                                 reviewer
                            WHERE review.rid = reviewer.rid AND 
                                 DATE(datetime) >= "%s"
                            ORDER BY DATE(datetime) DESC, rid ASC;''' %(start_date))
    #If only End date is specified
    elif 'end' in request.args.keys():
        end_date = str(request.args['end'])        
        cursor.execute('''SELECT comment,
                                 rid,
                                 datetime,
                                 accommodation_id,
                                 rname
                            FROM review
                                 NATURAL JOIN
                                 reviewer
                            WHERE review.rid = reviewer.rid AND 
                                 DATE(datetime) <= "%s" 
                            ORDER BY DATE(datetime) DESC, rid ASC;''' %(end_date))                                                                
    #If No date is specified
    else:
        cursor.execute('''SELECT comment,
                                 rid,
                                 datetime,
                                 accommodation_id,
                                 rname
                            FROM review
                                 NATURAL JOIN
                                 reviewer
                            WHERE review.rid = reviewer.rid
                            ORDER BY DATE(datetime) DESC, rid ASC;''')
                            
                            
    sqloutput = cursor.fetchall()
    output = list()
    for row in sqloutput:
        output.append({"Accommodation ID": row['accommodation_id']\
                       ,"Comment": row['comment']\
                       ,"DateTime": row['datetime']\
                       ,"Reviewer ID": row['rid']\
                       ,"Reviewer Name": row['rname']})
    output_count = len(output)
    dictoutput = {"Count":output_count,"Reviews":output}
    
    cursor.close()
    conn.close()
    return jsonify(dictoutput), 200, {'Content-Type': 'application/json'}



@app.route('/airbnb/reviewers/<reviewer_id>',methods = ['GET'])
@app.route('/airbnb/reviewers/',defaults={'reviewer_id': ''})
def get_reviewers(reviewer_id):
    conn = sqlite3.connect('airbnb.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    if reviewer_id != '':
            reviewer_id = int(reviewer_id)
            cursor.execute('''SELECT rid,
                                     rname,
                                     comment,
                                     datetime,
                                     accommodation_id
                                FROM review
                                     NATURAL JOIN
                                     reviewer
                                WHERE review.rid = "%d"
                                ORDER BY DATE(datetime) DESC;'''% (reviewer_id))
            sqloutput = cursor.fetchall()
            list_output = list()
            if not sqloutput:
                list_output.append({"Message":"Reviewer not found"})
                cursor.close()
                conn.close()
                return jsonify({"Reasons":list_output})\
                            , 404, {'Content-Type': 'application/json'}
            else:
                for row in sqloutput:
                    list_output.append({"Accommodation ID":row['accommodation_id']\
                                        ,"Comment": row['comment']\
                                        ,"DateTime": row['datetime']})
                        
            cursor.close()
            conn.close()
            return jsonify({"Reviewer ID":reviewer_id\
                            ,"Reviewer Name":row['rname']\
                            ,"Reviews":list_output})\
                            , 200, {'Content-Type': 'application/json'}
                              
    elif 'sort_by_review_count' in request.args.keys():
        sort_direction = str(request.args['sort_by_review_count'])
        if sort_direction == 'ascending':
            cursor.execute('''SELECT rid,
                                     rname,
                                     COUNT(*)
                                FROM review
                                     NATURAL JOIN
                                     reviewer
                                WHERE review.rid = reviewer.rid
                                GROUP BY rid
                                ORDER BY COUNT(*)ASC, rid;''')
                      
        elif sort_direction == 'descending':
            cursor.execute('''SELECT rid,
                                     rname,
                                     COUNT(*)
                                FROM review
                                     NATURAL JOIN
                                     reviewer
                                WHERE review.rid = reviewer.rid
                                GROUP BY rid
                                ORDER BY COUNT(*) DESC, rid;''')

    else:
        cursor.execute('''SELECT rid,
                               rname,
                               COUNT(*)
                          FROM review
                               NATURAL JOIN
                               reviewer
                          WHERE review.rid = reviewer.rid
                          GROUP BY rid
                          ORDER BY rid ASC;''')
                          
    sqloutput = cursor.fetchall()
    list_output = list()
    for row in sqloutput:
        list_output.append({"Review Count":row['COUNT(*)'], "Reviewer ID": row['rid'], "Reviewer Name": row['rname']})

    reviewer_count = len(list_output)
    
    cursor.close()
    conn.close()
    return jsonify({"Count":reviewer_count,"Reviewers":list_output}), 200, {'Content-Type': 'application/json'}



@app.route('/airbnb/hosts/<host_id>', methods = ['GET'])
@app.route('/airbnb/hosts/',defaults={'host_id': ''})
def get_hosts(host_id):
    conn = sqlite3.connect('airbnb.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    
    if host_id != '':
        host_id = int(host_id)
        cursor.execute(''' SELECT *
                              FROM host
                              NATURAL JOIN host_accommodation, accommodation
                              WHERE host.host_id == "%d"
                              AND host_accommodation.accommodation_id == accommodation.id;
                       '''%(host_id))
        sqloutput = cursor.fetchall()
        list_output = list()
        if sqloutput:
            for row in sqloutput:
                list_output.append({"Accommodation ID": row['accommodation_id']\
                                   ,"Accommodation Name": row['name']})
            cursor.close()
            conn.close()
            return jsonify({"Accommodation": list_output\
                            ,"Accommodation Count": len(list_output)\
                            , "Host About": sqloutput[0]['host_about']\
                            , "Host ID": sqloutput[0]['host_id']\
                            , "Host Location": sqloutput[0]['host_location']\
                            , "Host Name": sqloutput[0]['host_name']\
                            , "Host URL": sqloutput[0]['host_url']})\
                            , 200, {'Content-Type': 'application/json'}
        else:
            list_output.append({"Message":"Host not found"})
            
            cursor.close()
            conn.close()
            return jsonify({"Reasons":list_output})\
                            , 404, {'Content-Type': 'application/json'}
                            
                            
    elif 'sort_by_accommodation_count' in request.args.keys():
         sort_direction = str(request.args['sort_by_accommodation_count'])
         if sort_direction == 'ascending':
             cursor.execute('''SELECT *,COUNT(*)
                                  FROM host
                                  NATURAL JOIN host_accommodation
                                  WHERE host.host_id == host_accommodation.host_id
                                  GROUP BY host_id
                                  ORDER BY COUNT(*) ASC, host_id ASC;
                            ''')
         elif sort_direction == 'descending':                 
             cursor.execute('''SELECT *,COUNT(*)
                                 FROM host
                                 NATURAL JOIN host_accommodation
                                 WHERE host.host_id == host_accommodation.host_id
                                 GROUP BY host_id
                                 ORDER BY COUNT(*) DESC, host_id ASC;
                                ''')
                                
                                
    else:
        cursor.execute('''SELECT *,COUNT(*)
                                  FROM host
                                  NATURAL JOIN host_accommodation
                                  WHERE host.host_id == host_accommodation.host_id
                                  GROUP BY host_id
                                  ORDER BY host_id ASC;
                            ''')
    sqloutput = cursor.fetchall()
    list_output = list()
    for row in sqloutput:
        list_output.append({"Accommodation Count":row['COUNT(*)']\
                            , "Host About": row['host_about']\
                            , "Host ID": row['host_id']\
                            , "Host Location": row['host_location']\
                            , "Host Name": row['host_name']\
                            , "Host URL": row['host_url']})
    host_count = len(list_output)
    
    cursor.close()
    conn.close()
    return jsonify({"Count":host_count,"Hosts":list_output}), 200, {'Content-Type': 'application/json'}




@app.route('/airbnb/accommodations/<accommodation_id>', methods = ['GET'])
@app.route('/airbnb/accommodations/', defaults = {'accommodation_id': ''})
def get_accommodations(accommodation_id):
    conn = sqlite3.connect('airbnb.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()       
    if accommodation_id != '':
        accommodation_id = int(accommodation_id)
        cursor.execute('''SELECT * 
                            FROM accommodation
                            WHERE id == "%d" ''' %(accommodation_id))
        sqloutput = cursor.fetchall()
        accom_output = dict()
        if sqloutput:
            for row in sqloutput:
                accom_id = row['id']
                accom_name = row['name']
                accom_review_score = row['review_score_value']
                accom_summary = row['summary']
                accom_url = row['url']
                
                
                #Construct Amenities List
                amenities_list = list()
                cursor.execute('''SELECT * 
                                     FROM amenities WHERE accommodation_id == "%d"
                                     ORDER BY type ASC;
                                   '''%(accom_id))
                amenities_output = cursor.fetchall()
                for amen_row in amenities_output:
                    amenities_list.append(amen_row['type'])
                    
                    
                #Consturct Reviews List
                review_list = list()
                cursor.execute('''SELECT *
                                     FROM review
                                          JOIN
                                          reviewer ON review.rid == reviewer.rid
                                    WHERE accommodation_id == "%d"
                                    ORDER BY DATE(datetime) DESC;
                                   '''%(accom_id))
                review_output = cursor.fetchall()
                for review_row in review_output:
                    review_dict = dict()
                    review_dict["Comment"] = review_row['comment']
                    review_dict["DateTime"] = review_row['datetime']
                    review_dict["Reviewer ID"] = review_row['rid']
                    review_dict["Reviewer Name"] = review_row['rname']
                    review_list.append(review_dict)
                
                accom_output["Accommodation ID"] = accom_id
                accom_output["Accommodation Name"] = accom_name
                accom_output["Amenities"] = amenities_list
                accom_output["Review Score Value"] = accom_review_score
                accom_output["Reviews"] = review_list
                accom_output["Summary"] = accom_summary
                accom_output["URL"] = accom_url
                
            cursor.close()
            conn.close()
            return jsonify(accom_output)\
                           , 200, {'Content-Type': 'application/json'}  
        else:
            list_output = list()
            list_output.append({"Message":"Accommodation not found"})
            
            cursor.close()
            conn.close()
            return jsonify({"Reasons":list_output})\
                            , 404, {'Content-Type': 'application/json'}   
    
    elif ('min_review_score_value' in request.args.keys()) and ('amenities' in request.args.keys()):
        min_score = int(request.args['min_review_score_value'])
        request_amen = str(request.args['amenities'])
        cursor.execute('''SELECT name,summary,url,accommodation.id,COUNT(*),review_score_value
                            FROM accommodation 
                            LEFT JOIN review 
                            ON review.accommodation_id == accommodation.id
                            WHERE accommodation.id IN (SELECT accommodation_id FROM amenities WHERE type = "%s")
                            GROUP BY accommodation.id
                            HAVING review_score_value >= "%d"
                            ORDER BY accommodation.id ASC;''' %(request_amen,min_score))
    
    elif 'min_review_score_value' in request.args.keys():
        min_score = int(request.args['min_review_score_value'])
        cursor.execute('''SELECT name,summary,url,accommodation.id,COUNT(*),review_score_value
                            FROM accommodation 
                            LEFT JOIN review 
                            ON review.accommodation_id == accommodation.id
                            GROUP BY accommodation.id
                            HAVING review_score_value >= %d
                            ORDER BY accommodation.id ASC;''' %min_score)
    
    
    elif 'amenities' in request.args.keys():
        request_amen = str(request.args['amenities'])
        cursor.execute('''SELECT name,summary,url,accommodation.id,COUNT(*),review_score_value FROM accommodation 
                            LEFT JOIN review 
                            ON review.accommodation_id == accommodation.id
                            WHERE accommodation.id IN (SELECT accommodation_id FROM amenities WHERE type = "%s")
                            GROUP BY accommodation.id
                            ORDER BY accommodation.id ASC;''' %request_amen)
    
    else:
        cursor.execute('''SELECT name,summary,url,accommodation.id,COUNT(*),review_score_value FROM accommodation 
                            LEFT JOIN review 
                            ON review.accommodation_id == accommodation.id
                            GROUP BY accommodation.id
                            ORDER BY accommodation.id ASC;''')
                              
    
    sqloutput = cursor.fetchall()
    accom_list = list()
    for row in sqloutput:
        accom_info_dict = dict()
        accom_id = row['id']
        accom_review_score = row['review_score_value']
        if accom_review_score == None:
            accom_review_count = 0
        else:
            accom_review_count = row['COUNT(*)']
        
        #Construct Accommodation Dict
        accom_dict = {"Name": row['name']\
                      ,"Summary": row['summary']\
                      ,"URL": row['url']}
                    
            
        #Construct Amenities List
        amenities_list = list()
        cursor.execute('''SELECT * 
                           FROM amenities WHERE accommodation_id == "%d";
                           '''%(accom_id))
        amenities_output = cursor.fetchall()
        for amen_row in amenities_output:
            amenities_list.append(amen_row['type'])
        
        
        #Construct Host Dict
        cursor.execute('''SELECT *
                              FROM host
                              JOIN host_accommodation on
                              host.host_id == host_accommodation.host_id
                              WHERE accommodation_id == "%d";                          
                       '''%(accom_id))
        host_output = cursor.fetchone()
        host_dict = {"About": host_output['host_about']\
                     ,"ID": host_output['host_id']\
                     ,"Location": host_output['host_location']\
                     ,"Name": host_output['host_name']}
        
        #Put Info into Accomodation Info Dict
        accom_info_dict["Accommodation"] = accom_dict
        accom_info_dict["Amenities"] = amenities_list
        accom_info_dict["Host"] = host_dict
        accom_info_dict["ID"] = accom_id
        accom_info_dict["Review Count"] = accom_review_count
        accom_info_dict["Review Score Value"] = accom_review_score
        
        #Append Each Complete Accomodation Info into the list
        accom_list.append(accom_info_dict)
    cursor.close()
    conn.close()
    return jsonify({"Accommodations": accom_list, "Count": len(accom_list)})\
                    , 200, {'Content-Type': 'application/json'}

    
if __name__ == '__main__':
   app.run()


