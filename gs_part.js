function myFunction() {
  var ss = SpreadsheetApp.openByUrl("https://docs.google.com/spreadsheets/SOME_URL")

  var sheet = ss.getSheetByName('Ответы на форму (1)');
  var numRows = sheet.getLastRow();
  //Logger.log(numRows)

  var dataRange = sheet.getRange(1, 1, numRows, 7);
  var data = dataRange.getValues(); //Logger.log(data); 
  
  for (var i = 1; i < data.length; ++i) 
  {
    // GET DATA
    var row = data[i]; //Logger.log(row);
    var time_data = i; //Logger.log(time_data);
    var name = row[1]; //Logger.log("Name: " + name);
    var poscar = row[2]; //Logger.log("poscar: " + poscar);
    var calc_type = row[3] ; //Logger.log("calc_type: " + calc_type);
    var calc_type_1 = 'Расчет с релаксацией'
    var calc_type_2 = 'Расчет упругих постоянных'

    compare_1 = calc_type.localeCompare(calc_type_1); //Logger.log(compare_1);
    compare_2 = calc_type.localeCompare(calc_type_2); //Logger.log(compare_2);

    // Define type of calculation
    c_type = 'None'
    if ( compare_1 == 0.0 ){
      c_type = 'rx' ; // Logger.log('c_type:' + c_type)
    }
    if ( compare_2 == 0.0 ){
       c_type = 'elastic'
    }

    var regexp = /[а-яёА-ЯЁ]+/i;
    if(regexp.test(poscar)) {
       // @ts-ignore
       var poscar = 'False' //; Logger.log("poscar: "+poscar);
    }

    //// check if email already forwarded
    var email_fwd = row[6]; // Logger.log("Mailing status: " + email_fwd);

    if (email_fwd != 'email_fwd'){
      //Logger.log(name + " " + email_fwd);
      
      // MAIL FOR RESULTS
      var sheet0 = ss.getSheetByName('Список');
      var numRows0 = sheet0.getLastRow();
      var dataRange0 = sheet0.getRange(1, 1, numRows0, 2);
      var data0 = dataRange0.getValues();
      for (var j = 1; j < data0.length; ++j) 
      {
        var row0 = data0[j];
        var name0 = row0[0];

        compare = name0.localeCompare(name); //Logger.log(compare);

        if (compare == 0.0){
          var email = row0[1]; //Logger.log(email);
        }      
      }

      // SEND
      var name_translated = LanguageApp.translate(name, 'Ru', 'En'); //Logger.log(name_translated);
      var subject = 'HomeWork_1 ' + name_translated; //Logger.log(subject);
      var my_email = 'uifarm@mail.ru';

      var compose = '@&time:'    + time_data +
                    '@&name:'   + name_translated.split(' ')[0] + '_' + name_translated.split(' ')[1] +
                    '@&email:'  + email +
                    '@&poscar:' + poscar +
                    '@&ctype:' + c_type;
      //Logger.log(compose);

      MailApp.sendEmail(my_email, subject, compose); Logger.log('Files from ' + name_translated + ' was sent')
    
      // mark as forwarded
      num_cell = 'G' + (i + 1)
      email_fwd_cell = sheet.getRange(num_cell); 
      email_fwd_cell.setValue('email_fwd');
    }
  }
}
