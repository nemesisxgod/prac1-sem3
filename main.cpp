#include <iostream>
#include "header.h"
using namespace std;

int main() {
    TableJson jsontb;
    parser(jsontb);
    string command;
    while (true) {
        cout << "waiting for the command - ";
        getline(cin, command);
        if (command == "") {
            continue;
        }

        istringstream cmnd(command);
        string firstPart;
        cmnd >> firstPart;

        try {
            if (firstPart == "INSERT") {
                insert(command, jsontb);
            }
            else if (firstPart == "DELETE") {
                strDelete(command, jsontb);
            }
            else if (firstPart == "SELECT") {
                select(command, jsontb);
            }
            else if (firstPart == "EXIT") {
                return 0;
            }
            else cout<<"unknown command"<<endl;
        } catch (const exception& e) {
            cerr << "error: " << e.what() << endl;
        }
    }
}
