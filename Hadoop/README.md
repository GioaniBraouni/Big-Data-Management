The java code is written for hadoop version 3.3.0 in ubuntu

User Manual :

Κρατάμε σε μια μεταβλητή το path που έχουμε αποθηκεύσει στον 
υπολογιστή μας.
We store in a variable the path of the hadoop installed in our compouter.

1) export HADOOP_CLASSPATH=$(hadoop classpath)

Δημιουργούμε ένα φάκελο με όνομα Ergasia στο hadoop.
Create a folder in hadoop.
2) hadoop fs -mkdir /Ergasia

Ανεβάζουμε το αρχείο εισόδου στο hadoop.
Upload the input file.
3)hadoop fs -put personality_analysis.csv /Ergasia/Input

Κάνουμε compile το αρχείο java.
Compile the java source code.
4)javac -Xlint -classpath ${HADOOP_CLASSPATH} -d 
'/home/kali/Downloads/compiledFIles' '/home/kali/Downloads/app.java

Μετατρέπουμε το μεταγλωττισμένο αρχείο σε εκτελέσιμο.Η τελεία είναι σημαντική στο τέλος
Make the compiled file to executable.Dont forget the last full stop.
5)jar -cvf app.jar -C '/home/kali/Downloads/compiledFIles' .

Τρέχουμε το εκτελέσιμο αρχείο στο hadoop.
Run the exe file in the hadoop.
6)hadoop jar '/home/kali/Downloads/app.jar' app /Ergasia/Input 
/Ergasia/Output

Στο browser πληκτρολώντας http://localhost:9870 , αποκτάμε πρόσβαση στο hadoop.
Typing http://localhost:9870 in the browser top search bar , we have access in hadoop.
