#ifndef DATASET_H
#define DATASET_H
#include<string>
#include<string.h>
#include<vector>
using namespace std;


#define handle_error(en, msg) \
	do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)

// employee record
struct employeeRecord{
		int eid;
		string firstName;
		string gender;
		string middleName;
		string lastName;
		string cityName;
		string stateName;
		string ssNumber;

	public:
		employeeRecord(int id, string g, string fName, string mName, string lName, string city, string state, string ssn)
		:eid(id), gender(g), firstName(fName), middleName(mName), lastName(lName), cityName(city), stateName(state), ssNumber(ssn)
		{}
		employeeRecord(vector<string> v){
			eid = stoi(v[0]);
			gender = v[1];
			firstName = v[2];
			middleName = v[3];
			lastName = v[4];
			cityName = v[5];
			stateName = v[6];
			ssNumber = v[7];
		}
    
    //Accessors used to help get the private data members for comparison
    string getStateName() {return stateName;}
    int getEid() {return eid;}
    
    //returns the according attribute
    string getAttributes(int it)
    {
        switch(it)
        {
            case 1: return gender;
            case 2: return firstName;
            case 3: return middleName;
            case 4: return lastName;
            case 5: return cityName;
            case 6: return stateName;
            case 7: return ssNumber;
        }
    }
    
    void print() {cout << eid<<"-"<<gender<<"-"<<firstName<<"-"<<middleName<<"-"\
        <<lastName<<"-"<<cityName<<"-"<<stateName<<"-"<<ssNumber<<endl;}
};

// return value structure - frequency of the keyword and time spent in the thread
struct retVal{
	int frequency;
	double timeUsed;
};

// argument structure - starting index of the database, ending index of the database, keyword to search, thread number, sorting criteria and a pointer to the return
// value structure
struct argList {
    int startIndex;
	int endIndex;
	string keyword;
	int threadno;
	int minSize;
	vector<int> criteria;	
	retVal * rVal;
} ;

// argument structure - file name and a poiner to the return value structure, pass this structure when you create the readerThread and writerThread
struct argListforRW {
	string fileName;
	retVal * rVal;
} ;

// different states of the system

#define STATUS_INACTIVE 0
#define STATUS_READING  1
#define STATUS_READING_COMPLETED 2
#define STATUS_SORTING	      3
#define STATUS_SORTING_COMPLETED 4
#define STATUS_WRITING 5
#define STATUS_WRITING_COMPLETED 6	
#define STATUS_ABORT 7

// the list of employees
vector<employeeRecord*> dataSet;

#endif
