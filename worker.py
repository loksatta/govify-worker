import config
import time
import uuid
import subprocess
import os

import sendgrid

import pyrax
pyrax.set_setting("identity_type", "rackspace")
pyrax.set_setting("region",  config.rackspace['API_REGION'])


def do_main_program() :
    pyrax.set_credentials(config.rackspace['API_USERNAME'], config.rackspace['API_KEY'])

    my_client_id = str(uuid.uuid4())

    pq = pyrax.queues
    pq.client_id = my_client_id

    cf = pyrax.cloudfiles
    in_container = cf.get_container(config.rackspace['API_FILES_IN'])
    out_container = cf.get_container(config.rackspace['API_FILES_OUT'])

    # We set the ttl and grace period to their minimum, 60 seconds.
    # Get 1 at a time.
    claim = pq.claim_messages(config.rackspace['API_QUEUE'], 60, 60, 1)

    if claim and len(claim.messages) :
        for msg in claim.messages:
            #print 'Claimed {0}'.format([msg.body])
            in_obj = in_container.get_object(msg.body['Tempname'])

            # Generate a safe filename.
            new_filename = '/tmp/' + msg.body['Tempname']

            # Insert our data into that file.
            f = open(new_filename, 'w')
            f.write(in_obj.get())
            f.close()

            # If we successfully govify'd the document
            try:
                subprocess.check_call(["/usr/bin/govify", new_filename])

                f = open(new_filename + '.pdf', 'r')

                # Upload the new file
                obj = out_container.store_object(msg.body['Tempname'] + '.pdf', f.read(),
                    content_type='application/pdf', ttl=config.rackspace['API_FILE_LIFETIME'])

                f.close()

                os.remove(new_filename + '.pdf')

                # Remove the item from the inbox
                # Do this via the container so the cache is cleared!
                in_container.delete_object(msg.body['Tempname'])

                # Remove the item from the queue
                pq.delete_message(config.rackspace['API_QUEUE'], msg.id, claim.id)

                # Notify the user
                do_mail(msg.body['Author'], obj.get_temp_url(config.rackspace['API_FILE_LIFETIME']));


            except subprocess.CalledProcessError:
                print 'Something went wrong!'

            # Remove our temp files.
            os.remove(new_filename)

def do_mail(email, link) :
    msg = """Howdy!  Your .gov.ify processed file is now available at the following URL:

{0}

Though the file is real, this whole thing is actually an April Fool's prank from OpenGov
Foundation.  If you'd like to check out our other work, head on over to our website:

http://opengovfoundation.org/

""".format(link)

    sg = sendgrid.SendGridClient(config.sendgrid['USERNAME'], config.sendgrid['PASSWORD'])

    message = sendgrid.Mail(to=email, subject='Your .gov.ify file has been processed', html=msg.replace('\n', '<br />'), text=msg, from_email=config.sendgrid['FROM'])
    status, msg = sg.send(message)

def run():
    while True:
        do_main_program()
        time.sleep(config.loop_sleep)

run()

# with daemon.DaemonContext():
#     do_main_program()
#     time.sleep(15)
